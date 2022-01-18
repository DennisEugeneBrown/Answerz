from datetime import datetime

from answerz.model.QueryBlock import QueryBlock
from answerz.decoder.entity.EntityDecoderBase import EntityDecoderBase


class DateRangeEntityDecoder(EntityDecoderBase):
    def __init__(self, data_map):
        self.data_map = data_map

    # Takes the entity to decode + a potential query_block to augment
    def decode(self, entity, query_block=None, comparator=None, string_operators=None):
        value = entity['entity']

        if value["type"] == "daterange":

            entity_type = "datetime"
            entity_value = value['values'][0]['resolution'][0]

            lu = self.lookupTablesAndFieldByType(
                query_block.queryIntent[0], query_block.queryIntent[1], entity_type, self.data_map)

            #############################

            if lu:
                tables = lu["tables"]
                field_name = lu["field"]
                display_name = lu["display_name"] if "display_name" in lu else ''
            else:
                tables = []
                field_name = ''
                display_name = ''

            qb = QueryBlock(query_block.queryIntent)

            if "start" in entity_value:
                condition = ["gte", field_name, entity_value["start"]]
                qb.conditions.append(condition)
                if entity['text'] in qb.date_range_conditions:
                    qb.date_range_conditions[entity['text']].append(condition)
                else:
                    qb.date_range_conditions[entity['text']] = [condition]

            if "end" in entity_value:
                condition = ["lt", field_name, entity_value["end"]]
            else:
                condition = ["lt", field_name, datetime.now().strftime('%Y-%m-%d')]
            qb.conditions.append(condition)

            if entity['text'] in qb.date_range_conditions:
                qb.date_range_conditions[entity['text']].append(condition)
            else:
                qb.date_range_conditions[entity['text']] = [condition]

            if "joins" in lu and lu["joins"]:
                for join in lu["joins"]:
                    qb.addTable(join[0], join[1])

            return qb

        return None
