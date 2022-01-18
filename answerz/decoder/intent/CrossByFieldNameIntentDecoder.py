from answerz.model.QueryBlock import QueryBlock
from answerz.model.DataMapRepo import DataMapRepo


class CrossByFieldNameIntentDecoder:
    def __init__(self, data_map):
        self.data_map = data_map
        self._element = 'Calls'
        self._aggregation = 'Count'

    def findEntityByType(self, entities, type_name):
        for ix, e in enumerate(entities):
            if e["type"] == type_name:
                return ix, e['entity']
        return -1, None

    def findFieldNames(self, entities):
        fieldNames = []
        for e in entities:
            if e["type"] == '_FieldName':
                fieldNames.append(e['entity'])
        return fieldNames

    def decode(self, intent_name, entities, prev_q=None, is_a_prev_query=False):
        # global DATA_MAP

        _groupAction_ix, _groupAction = self.findEntityByType(entities, "_GroupAction")

        _fieldNames = self.findFieldNames(entities)

        data_map_repo = DataMapRepo(self.data_map)
        mapped_element, mapped_aggregation = data_map_repo.findMapping(
            self._element, self._aggregation)

        update_number = None
        for field_name in _fieldNames:
            for dim in mapped_element["Dimensions"]:
                if dim["name"].lower() == field_name.lower() and dim['type'] == 'int':
                    update_number = dim["name"]

        if update_number:
            for ix, entity in enumerate(entities):
                if entity['type'] == 'builtin.number':
                    entities[ix]['type'] = update_number

        qb = QueryBlock((self._element, self._aggregation))

        qb.is_cross = True

        for table in mapped_aggregation["tables"]:
            if type(table) == str:
                qb.addTable(table)
            else:
                qb.addTable(table[0], table[1])

        for col in mapped_aggregation["columns"]:
            if "type" in col and col["type"] == "agg":
                if "field" in col and col["field"]:
                    if col["distinct"]:
                        qb.selects.append(["Count(distinct dbo.{}.{})".format(
                            qb.tables[0], col["field"]), qb.queryIntent[0]])
                    else:
                        qb.selects.append(
                            ["Count({})".format(col["field"]), qb.queryIntent[0]])
                else:
                    qb.selects.append(["Count()", "Count"])

        return entities, qb
