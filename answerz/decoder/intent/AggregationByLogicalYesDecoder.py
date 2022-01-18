from answerz.model.QueryBlock import QueryBlock
from answerz.model.DataMapRepo import DataMapRepo


class AggregationByLogicalYesDecoder:
    def __init__(self, data_map):
        self.data_map = data_map

    def findEntityByType(self, entities, type_name):
        for ix, e in enumerate(entities):
            if e["type"] == type_name:
                return ix, e['entity']
        return -1, None

    # We pass the entire list of entities to the decoder although we expect most to be ignored here
    def decode(self, intent_name, entities, prev_q=None, is_a_prev_query=False):
        # global DATA_MAP

        _element_ix, _element = self.findEntityByType(entities, "_DataElement")
        _aggregation_ix, _aggregation = self.findEntityByType(entities, "_Aggregations")
        _logicalLabel_ix, _logicalLabel = self.findEntityByType(entities, "_LogicalLabel")
        _groupAction_ix, _groupAction = self.findEntityByType(entities, "_GroupAction")
        _fieldName_ix, _fieldName = self.findEntityByType(entities, "_FieldName")

        data_map_repo = DataMapRepo(self.data_map)
        _, mapped_aggregation = data_map_repo.findMapping(
            _element, _aggregation)

        if _groupAction:
            _, mapped_grouping = data_map_repo.findGrouping(
                _element, _fieldName)

        qb = QueryBlock((_element, _aggregation))
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
                            qb.selects.append(["Count(distinct {})".format(
                                col["field"]), qb.queryIntent[0]])
                        else:
                            qb.selects.append(
                                ["Count({})".format(col["field"]), qb.queryIntent[0]])
                    else:
                        qb.selects.append(["Count()", "Count"])

        if _groupAction:
            if mapped_grouping['joins']:
                qb.joins.extend(tuple(mapped_grouping['joins']))
            qb.groups.append(
                (mapped_grouping['field'], mapped_grouping['name']))

        # qb.addTable("CallLog")

        return entities, qb
