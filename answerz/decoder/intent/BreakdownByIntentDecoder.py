from answerz.model.QueryBlock import QueryBlock
from answerz.model.DataMapRepo import DataMapRepo
from answerz.utils.entity_utils import handle_groupings


class BreakdownByIntentDecoder:
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
        fieldNames = []
        fieldName_entities = []
        for e in entities:
            if e["type"] == '_FieldName':
                fieldNames.append(e['entity'])
                fieldName_entities.append(e)
        return fieldNames, fieldName_entities

    # We pass the entire list of entities to the decoder although we expect most to be ignored here
    def decode(self, intent_name, entities, prev_q=None, is_a_prev_query=False):
        qb = prev_q
        try:
            _element = qb.queryIntent[0]  # TODO: Add and use a getter
            _aggregation = qb.queryIntent[1]  # TODO: Add and use a getter
        except AttributeError:
            print('ERROR. CANNOT FIND PREVIOUS QUERY TO BREAKDOWN.')
            return QueryBlock()

        _groupAction_ix, _groupAction = self.findEntityByType(entities, "_GroupAction", return_entity=True)
        _logicalLabel_ix, _logicalLabel = self.findEntityByType(entities, "_LogicalLabel")
        _fieldNames, _fieldName_entities = self.findFieldNames(entities)

        data_map_repo = DataMapRepo(self.data_map)
        _, mapped_aggregation = data_map_repo.findMapping(_element, _aggregation)

        mapped_groupings = []
        if _groupAction:
            if _groupAction['entity'].lower() in ['group by', 'breakdown by', 'by', 'grouped by']:
                if _fieldNames:
                    for ix, _fieldName in enumerate(_fieldNames):
                        if _fieldName_entities[ix]['startIndex'] > _groupAction['startIndex']:
                            _, mapped_grouping = data_map_repo.findGrouping(
                                _element, _fieldName)
                            mapped_groupings.append(mapped_grouping)
                            # break
                elif _logicalLabel:
                    _, mapped_grouping = data_map_repo.findGrouping(
                        _element, _logicalLabel)
                    mapped_groupings = [mapped_grouping]
                    del entities[_logicalLabel_ix]
                else:
                    pass
            elif _groupAction['entity'].lower() == 'compare':
                qb.is_compare = True

        for mapped_grouping in mapped_groupings:
            if mapped_grouping['joins']:
                qb.joins.extend(tuple(mapped_grouping['joins']))
            if 'sort_fields' in mapped_grouping:
                for sort_field in mapped_grouping['sort_fields']:
                    if qb.sorts and sort_field not in qb.sorts:
                        qb.sorts.append(sort_field)
                    if sort_field[0] != mapped_grouping['field']:
                        qb.groups.append((sort_field[0], sort_field[0]))
            else:
                if qb.sorts and (mapped_grouping['field'], 'ASC') not in qb.sorts:
                    qb.sorts.append((mapped_grouping['field'], 'ASC'))

        if _groupAction['entity'].lower() in ['group by', 'breakdown by'] and mapped_groupings and (
                _fieldNames or _logicalLabel):
            qb = handle_groupings(mapped_groupings, qb)
            qb.selects.append([
                "CAST(CAST({count} * 100.0 * 2.0 / sum({count}) over () AS decimal(10, 2)) AS varchar) + '%'".format(
                    count=qb.selects[0][0]),
                'Total %'])

        return entities, qb
