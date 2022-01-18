import us

from answerz.model.QueryBlock import QueryBlock
from answerz.decoder.entity.EntityDecoderBase import EntityDecoderBase


class ColumnEntityDecoder(EntityDecoderBase):
    def __init__(self, data_map):
        self.data_map = data_map
        pass

    def mapValues(self, table, field_name, entity_value):
        return [entity_value]

    # Takes the entity to decode + a potential query_block to augment
    def decode(self, entity, query_block, comparator=None, string_operators=None):
        # print(entity)
        values = [entity['entity']]
        if isinstance(entity['entity'], dict) and 'values' in entity['entity'] and 'resolution' in \
                entity['entity']['values'][0]:
            values = entity['entity']['values'][0]["resolution"]

        if len(values) == 1:

            entity_name = entity["type"]
            value = values[0] if isinstance(values, list) else values
            entity_value = (value['value'].title() if 'value' in value else value['resolution'][0][
                'value'].title()) if isinstance(value, dict) else value

            lu = self.lookupTablesAndField(
                query_block.queryIntent[0], query_block.queryIntent[1], entity_name, self.data_map)

            if lu:
                tables = lu["tables"]
                field_name = lu["field"]
                display_name = lu["display_name"] if "display_name" in lu else ''
            else:
                tables = []
                field_name = ''
                display_name = ''

            if display_name == 'State':  # Using state abbreviations in queries instead of state names
                state_name = entity_value.replace(
                    ' State', '').replace(' state', '')
                print('Looking up state: {}..'.format(state_name))
                entity_value = us.states.lookup(state_name).abbr

            if display_name == 'City' or field_name == 'builtin.geographyV2.city':
                entity_value = entity_value.replace(
                    ' City', '').replace(' city', '')

            if display_name == 'County':
                entity_value = entity_value.replace(
                    ' County', '').replace(' county', '')

            exact_match = True
            choice_for_field = {}
            if lu and 'exact_match' in lu and lu['exact_match'] is False:
                exact_match = False
                choice = lu['choice'] if 'choice' in lu else False
                if "." in field_name:  # already scoped
                    choice_for_field[lu['field']] = choice
                else:
                    choice_for_field[lu['tables'][0] + '.' + lu['field']] = choice

            qb = QueryBlock(query_block.queryIntent)

            qb.choice_for_field = choice_for_field

            print(tables)
            for table in tables:
                if type(table) == str:
                    qb.addTable(table)
                else:
                    # note: this is not yet tested and may break
                    qb.addTable(table[0], table[1])
            if tables:
                db_values = self.mapValues(tables[0], field_name, entity_value)
                if len(db_values) == 1:

                    if string_operators:
                        if "." in field_name:  # already scoped
                            s = string_operators[0].format(field_name, entity_value)
                            s = s.split(' ')
                            condition = [s[1], s[0], s[2]]
                        else:
                            s = string_operators[0].format(tables[0] + "." + field_name, entity_value)
                            s = s.split(' ')
                            condition = [s[1], s[0], s[2]]
                    elif exact_match:
                        if "." in field_name:  # already scoped
                            condition = [(comparator if comparator else "eq"), field_name, entity_value]
                        else:
                            condition = [(comparator if comparator else "eq"), tables[0] + "." + field_name,
                                         entity_value]
                    else:
                        if "." in field_name:  # already scoped
                            condition = ["lk", field_name, '%' + entity_value + '%']
                        else:
                            condition = ["lk", tables[0] + "." + field_name, '%' + entity_value + '%']

                    # qb.selects.append(self.generateConditionalCountSelect(condition))
                    qb.conditions.append(condition)
                    qb.condition_locs[condition[-1]] = entity['startIndex']

                    if entity['type'] in qb.conditions_by_type:
                        qb.conditions_by_type[entity['type']].append(condition)
                    else:
                        qb.conditions_by_type[entity['type']] = [condition]

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
                    return None

            return qb

        else:
            print("Duplicate value types unhandled")
            print("Duplicate value types unhandled")

        return None
