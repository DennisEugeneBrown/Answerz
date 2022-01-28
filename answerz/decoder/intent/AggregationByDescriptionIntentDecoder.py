from answerz.model.QueryBlock import QueryBlock
from answerz.model.DataMapRepo import DataMapRepo
from answerz.utils.entity_utils import findFieldNames, findEntityByType, handle_number_entities, handle_groupings


class AggregationByDescriptionIntentDecoder:
    def __init__(self, data_map):
        self.data_map = data_map

    # We pass the entire list of entities to the decoder, although we expect most to be ignored here

    def decode(self, intent_name, entities, prev_q=None, is_a_prev_query=False):
        # global DATA_MAP

        _element_ix, _element = findEntityByType(entities, "_DataElement")
        _aggregation_ix, _aggregation = findEntityByType(entities, "_Aggregations")
        _logicalLabel_ix, _logicalLabel = findEntityByType(entities, "_LogicalLabel")
        _groupAction_ix, _groupAction = findEntityByType(entities, "_GroupAction", return_entity=True)
        _comparators = findEntityByType(entities, "_Comparator", return_entity=True, return_many=True)
        _conditionSeparator_ix, _condition_Separator = findEntityByType(entities, "_ConditionSeparator",
                                                                        return_entity=True)
        _stringOperator_ix, _stringOperator = findEntityByType(entities, "_StringOperators")
        _field_names, _field_name_entities = findFieldNames(entities)

        data_map_repo = DataMapRepo(self.data_map)
        mapped_element, mapped_aggregation = data_map_repo.findMapping(_element, _aggregation)

        entities = handle_number_entities(entities, mapped_element)

        qb = QueryBlock((_element, _aggregation))

        if entities[_aggregation_ix]['text'].lower() == 'compare':
            qb.is_compare = True

        if _condition_Separator:
            qb.cond_sep[_condition_Separator['startIndex']] = _condition_Separator['entity']

        comparators_mapping = {
            '<': 'lt',
            '>': 'gt',
            '<=': 'lte',
            '>=': 'gte',
            '!=': 'not'
        }

        comparators_position_mapping = {
            'or more': 'after',
            'or less': 'after',
            'is not': 'after'
        }

        string_operators_mapping = {
            'startsWith': "{} lk {}%",
            'endsWith': "{} lk %{}",
            'contains': "{} lk %{}%",
        }

        for _comparatorIx, _comparatorEntity in _comparators:
            _comparatorEntity['position'] = comparators_position_mapping[_comparatorEntity['text'].lower()] if \
                _comparatorEntity['text'].lower() in comparators_position_mapping else 'before'
            qb.comparators.append((comparators_mapping[_comparatorEntity['entity']], _comparatorEntity))

        if _stringOperator:
            qb.string_operators.append(string_operators_mapping[_stringOperator])

        mapped_groupings = []
        default_grouping = False
        if _groupAction and not is_a_prev_query:
            if _groupAction['entity'].lower() in ['group by', 'breakdown by', 'by', 'grouped by']:
                if _field_names:
                    for ix, _fieldName in enumerate(_field_names):
                        if _field_name_entities[ix]['startIndex'] > _groupAction['startIndex']:
                            _, mapped_grouping = data_map_repo.findGrouping(_element, _fieldName)
                            mapped_groupings.append(mapped_grouping)
                            # break
                elif _logicalLabel:
                    _, mapped_grouping = data_map_repo.findGrouping(_element, _logicalLabel)
                    mapped_groupings = [mapped_grouping]
                    del entities[_logicalLabel_ix]
                else:
                    _, mapped_grouping = data_map_repo.findGrouping(
                        _element, 'Year')
                    mapped_groupings.append(mapped_grouping)
                    _groupAction = {'entity': 'group by'}
                    default_grouping = True
            elif _groupAction['entity'].lower() == 'compare':
                qb.is_compare = True
        elif not _groupAction and not is_a_prev_query:
            _, mapped_grouping = data_map_repo.findGrouping(
                _element, 'Year')
            mapped_groupings.append(mapped_grouping)
            _groupAction = {'entity': 'group by'}
            default_grouping = True

        for table in mapped_aggregation["tables"]:
            if type(table) == str:
                qb.addTable(table)
            else:
                qb.addTable(table[0], table[1])

        for col in mapped_aggregation["columns"]:
            if "type" in col and col["type"] == "agg":
                if col["agg"] == "count":
                    if "field" in col and col["field"]:
                        if col["distinct"]:
                            qb.selects.append(["Count(distinct dbo.{}.{})".format(
                                qb.tables[0], col["field"]), qb.queryIntent[0]])
                        else:
                            qb.selects.append(
                                ["Count({})".format(col["field"]), qb.queryIntent[0]])
                        with_qb = QueryBlock()
                        with_qb.addTable(table)
                        with_qb.joins = qb.joins
                        qb.with_query = with_qb
                    else:
                        qb.selects.append(["Count()", "Count"])
                elif col['agg'] == 'avg':
                    if "field" in col and col["field"]:
                        if 'cast' in col and col['cast']:
                            field = "CAST({}dbo.{}.{} AS {})".format('distinct ' if col['distinct'] else '',
                                                                     qb.tables[0],
                                                                     col["field"], col['cast'])
                        else:
                            field = "{}dbo.{}.{}".format('distinct ' if col['distinct'] else '', qb.tables[0],
                                                         col["field"])

                        qb.selects.append(["Avg({})".format(field), "Avg_" + qb.queryIntent[0]['field']])

        if _groupAction:
            if _groupAction['entity'].lower() in ['group by', 'breakdown by'] and mapped_groupings and (
                    _field_names or _logicalLabel or default_grouping):
                qb = handle_groupings(mapped_groupings, qb)
                qb.selects.append([
                    "CAST(CAST({count} * 100.0 / sum({count}) over () AS decimal(10, 2)) AS varchar) + '%'".format(
                        count=qb.selects[0][0]),
                    'Percentage'])
        return entities, qb
