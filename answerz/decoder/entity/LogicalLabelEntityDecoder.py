from answerz.model.QueryBlock import QueryBlock
from answerz.decoder.entity.EntityDecoderBase import EntityDecoderBase


class LogicalLabelEntityDecoder(EntityDecoderBase):
    def __init__(self, data_map):
        self.data_map = data_map
        pass

    def mapValues(self, table, field_name, entity_value):
        return [entity_value]

    # Takes the entity to decode + a potential query_block to augment
    def decode(self, entity, query_block, comparator=None, string_operators=None):
        # print(entity)
        if 'resolution' in entity:
            values = entity["resolution"]["values"]
        else:
            values = [entity]

        if len(values) == 1:

            entity_name = entity["type"]
            if 'resolution' in entity:
                entity_value = entity['entity']
            else:
                entity_value = entity['entity']

            lu = self.lookupTablesAndField(
                query_block.queryIntent[0], query_block.queryIntent[1], entity_name, self.data_map)

            tables = lu["tables"]
            field_name = lu['field'][entity_value.lower()]

            if comparator and 'not' in comparator:
                if 'default_negative_value' in lu:
                    entity_value = lu['default_negative_value']
                else:
                    entity_value = 'NO'
            elif 'default_positive_value' in lu:
                entity_value = lu['default_positive_value']
            else:
                entity_value = 'YES'

            exact_match = True
            choice_for_field = {}
            if 'exact_match' in lu and lu['exact_match'] == False:
                exact_match = False
                choice = lu['choice'] if 'choice' in lu else False
                if "." in field_name:  # already scoped
                    choice_for_field[lu['field'][entity['entity']]] = choice
                else:
                    choice_for_field[lu['tables'][0] + '.' + lu['field'][entity['entity'].lower()]] = choice

            qb = QueryBlock(query_block.queryIntent)
            qb.logical_label = True
            qb.choice_for_field = choice_for_field

            for table in tables:
                if type(table) == str:
                    qb.addTable(table)
                else:
                    # note: this is not yet tested and may break
                    qb.addTable(table[0], table[1])

            db_values = self.mapValues(tables[0], field_name, entity_value)
            if len(db_values) == 1:
                if string_operators:
                    if "." in field_name:  # already scoped
                        condition = [(string_operators[0].format(field_name, entity_value)), '', '']
                    else:
                        condition = [(string_operators[0].format(tables[0] + "." + field_name, entity_value)), '', '']
                elif exact_match:
                    if "." in field_name:  # already scoped
                        condition = ["eq", field_name, entity_value]
                    else:
                        condition = ["eq", tables[0] + "." + field_name, entity_value]
                else:
                    if "." in field_name:  # already scoped
                        condition = ["lk", field_name, '%' + entity_value + '%']
                    else:
                        condition = ["lk", tables[0] + "." + field_name, '%' + entity_value + '%']

                qb.conditions.append(condition)
                if entity['type'] in qb.conditions_by_type:
                    qb.conditions_by_type[lu['field'][entity['entity'].lower()]].append(condition)
                else:
                    qb.conditions_by_type[lu['field'][entity['entity'].lower()]] = [condition]

                if 'category' not in lu:
                    lu['category'] = entity['type']

                if lu['category'] in qb.conditions_by_category:
                    qb.conditions_by_category[lu['category']].append(condition)
                else:
                    qb.conditions_by_category[lu['category']] = [condition]

                if "joins" in lu and lu["joins"]:
                    for join in lu["joins"]:
                        qb.addTable(join[0], join[1])

                return qb

            else:
                print("Multiple value mapping not supported yet")
                return qb

        else:
            print("Duplicate value types unhandled")
            print("Duplicate value types unhandled")

        return None
