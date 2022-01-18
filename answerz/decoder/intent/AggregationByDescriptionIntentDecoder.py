from answerz.model.QueryBlock import QueryBlock
from answerz.model.DataMapRepo import DataMapRepo


class AggregationByDescriptionIntentDecoder:
    def __init__(self, data_map):
        self.data_map = data_map

    def findEntityByType(self, entities, type_name, return_entity=False, return_many=False):
        ret = []
        for ix, e in enumerate(entities):
            if e["type"] == type_name:
                if return_many:
                    ret.append((ix, e if return_entity else e['entity']))
                else:
                    return ix, e if return_entity else e['entity']
        if return_many:
            return ret
        return -1, None

    def findFieldNames(self, entities):
        field_names = []
        field_name_entities = []
        for e in entities:
            if e["type"] == '_FieldName':
                field_names.append(e['entity'])
                field_name_entities.append(e)
        return field_names, field_name_entities

    # We pass the entire list of entities to the decoder although we expect most to be ignored here

    def decode(self, intent_name, entities, prev_q=None, is_a_prev_query=False):
        # global DATA_MAP

        _element_ix, _element = self.findEntityByType(entities, "_DataElement")
        _aggregation_ix, _aggregation = self.findEntityByType(entities, "_Aggregations")
        _logicalLabel_ix, _logicalLabel = self.findEntityByType(entities, "_LogicalLabel")
        _groupAction_ix, _groupAction = self.findEntityByType(entities, "_GroupAction", return_entity=True)
        _comparators = self.findEntityByType(entities, "_Comparator", return_entity=True, return_many=True)
        _conditionSeparator_ix, _condition_Separator = self.findEntityByType(entities, "_ConditionSeparator",
                                                                             return_entity=True)
        _stringOperator_ix, _stringOperator = self.findEntityByType(entities, "_StringOperators")

        _field_names, _field_name_entities = self.findFieldNames(entities)

        # priority for fieldnames always

        for _fieldName in _field_name_entities:
            start_index = _fieldName['startIndex']
            end_index = _fieldName['startIndex'] + _fieldName['length']
            ixs_to_remove = []
            for ix, entity in enumerate(entities):
                if entity == _fieldName:
                    continue
                entity_start_index = entity['startIndex']
                entity_end_index = entity['startIndex'] + entity['length']
                if entity_start_index >= start_index and entity_end_index <= end_index:
                    ixs_to_remove.append(ix)
            for ix in reversed(ixs_to_remove):
                entities.pop(ix)

        data_map_repo = DataMapRepo(self.data_map)
        mapped_element, mapped_aggregation = data_map_repo.findMapping(
            _element, _aggregation)

        numbers = []
        for ix, field_name in enumerate(_field_names):
            for dim in mapped_element["Dimensions"]:
                if dim["name"].lower() == field_name.lower() and dim['type'] == 'int':
                    numbers.append((dim["name"], ix))

        number_type_entities = [(entity, ix) for ix, entity in enumerate(entities) if
                                entity['type'] == 'builtin.number']
        if len(numbers) == 1 and len(number_type_entities) == 1:
            entities[number_type_entities[0][1]]['type'] = numbers[0][0]
        else:
            numbers_covered = []
            # TODO: Improve numbers by looking at comparators rather than just distance
            for number, num_entity_ix in numbers:
                for ix, entity in enumerate(entities):
                    _fieldName_entity = _field_name_entities[num_entity_ix]
                    if entity['type'] == 'builtin.number' and (
                            abs((_fieldName_entity['startIndex'] + _fieldName_entity['length']) - entity[
                                'startIndex']) <= 3):
                        entities[ix]['type'] = number
                        numbers_covered.append(ix)
            for ix, entity in enumerate(entities):
                if entity['type'] == 'builtin.number' and ix not in numbers_covered:
                    entities[ix]['type'] = 'age'

        number_type_columns = [dim['name'].lower() for dim in mapped_element['Dimensions'] if dim['type'] == 'int']
        entity_types = [ent['type'].lower() for ent in entities]
        num_present = False
        for col in number_type_columns:
            if col in entity_types:
                num_present = True
                break
        for ix, entity in enumerate(entities):
            if entity['type'] == 'builtin.number' and not num_present:
                entities[ix]['type'] = 'age'

        qb = QueryBlock((_element, _aggregation))

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
                for mapped_grouping in mapped_groupings:
                    if mapped_grouping['joins']:
                        qb.joins.extend(tuple(mapped_grouping['joins']))
                    if 'display_name' in mapped_grouping:
                        qb.groups.append(
                            (mapped_grouping['field'], mapped_grouping['display_name']))
                    else:
                        qb.groups.append(
                            (mapped_grouping['field'], mapped_grouping['name']))
                    if 'sort_fields' in mapped_grouping:
                        for sort_field in mapped_grouping['sort_fields']:
                            if qb.sorts and sort_field not in qb.sorts:
                                qb.sorts.append(sort_field)
                            if sort_field[0] != mapped_grouping['field']:
                                qb.groups.append((sort_field[0], sort_field[0]))
                    else:
                        if qb.sorts and (qb.selects[0][-1], 'DESC') not in qb.sorts:
                            qb.sorts.append((qb.selects[0][-1], 'DESC'))
                qb.selects.append([
                    "CAST(CAST({count} * 100.0 / sum({count}) over () AS decimal(10, 2)) AS varchar) + '%'".format(
                        count=qb.selects[0][0]),
                    'Percentage'])
        return entities, qb
