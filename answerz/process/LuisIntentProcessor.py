import copy
import itertools
from pprint import pprint
from collections import defaultdict

from answerz.model.QueryBlock import QueryBlock

from answerz.utils.luis_utils import interpret

from answerz.utils.entity_utils import findFieldNames

from answerz.decoder.entity.DateEntityDecoder import DateEntityDecoder
from answerz.decoder.entity.ColumnEntityDecoder import ColumnEntityDecoder
from answerz.decoder.entity.DateRangeEntityDecoder import DateRangeEntityDecoder
from answerz.decoder.entity.LogicalLabelEntityDecoder import LogicalLabelEntityDecoder

from answerz.decoder.intent.BreakdownByIntentDecoder import BreakdownByIntentDecoder
from answerz.decoder.intent.CrossByFieldNameIntentDecoder import CrossByFieldNameIntentDecoder
from answerz.decoder.intent.AggregationByLogicalYesDecoder import AggregationByLogicalYesDecoder
from answerz.decoder.intent.AggregationByDescriptionIntentDecoder import AggregationByDescriptionIntentDecoder


class LuisIntentProcessor:

    def __init__(self, data_map, luis_config):
        # Intent Decoders
        self.data_map = data_map
        self.luis_config = luis_config
        self.i_decoders = {"agg-elements-by-description": AggregationByDescriptionIntentDecoder(
            data_map), "agg-elements-by-logical-yes": AggregationByLogicalYesDecoder(
            data_map), "breakdown-by": BreakdownByIntentDecoder(
            data_map), "cross-by-field-name": CrossByFieldNameIntentDecoder(
            data_map)}

        # Entity Decoders
        self.e_decoder_default = ColumnEntityDecoder(data_map)
        self.e_decoders = {"builtin.datetimeV2.daterange": DateRangeEntityDecoder(
            data_map), "builtin.datetimeV2.date": DateEntityDecoder(
            data_map), "_LogicalLabel": LogicalLabelEntityDecoder(
            data_map)}

        self.geo_transformations = {
            'County': 'county',
            'builtin.geographyV2.state': 'state',
            'State': 'state',
            'builtin.geographyV2.city': 'city',
            'City': 'city',
            'county': 'county',
            'state': 'state',
            'city': 'city',
        }
        self.geography_entity_types = ['geographyV2', 'County', 'City', 'State']
        self.date_entity_types = ['datetimeV2', 'builtin.datetimeV2',
                                  'builtin.datetimeV2.daterange']  # TODO: test dates
        self.number_entity_types = ['builtin.number']  # TODO: test dates

    def get_intent_decoder(self, intent_name):

        if intent_name in self.i_decoders:
            return self.i_decoders[intent_name]

        return None

    def get_field_values(self, field, table, query_processor):
        return query_processor.run_query('SELECT DISTINCT {} FROM {}'.format(field, table), [[field, field]])

    def get_entity_decoder(self, entity):

        t = entity["type"]
        if t in self.e_decoders:
            return self.e_decoders[t]

        if t.startswith("_"):
            # this is a system field
            return None

        return self.e_decoder_default

    def generateDistinctValuesQuery(self, qb):
        new_qb = QueryBlock()
        new_qb.tables = qb.tables[0:1]
        new_qb.conditions = qb.conditions
        new_qb.conditions_by_category = {}
        for category_, conds in qb.conditions_by_category.items():
            for cond in conds:
                if category_ in new_qb.conditions_by_category:
                    new_qb.conditions_by_category[category_].append(cond)
                else:
                    new_qb.conditions_by_category[category_] = [cond]
        field_name = new_qb.conditions[0][1] if new_qb.conditions[0][1] else new_qb.conditions[0][0].split(' like ')[0]
        new_qb.selects = [['COUNT(*)', field_name]]
        new_qb.groups = [[field_name, field_name]]
        new_qb.joins = qb.joins
        new_qb.date_range_conditions = qb.date_range_conditions
        new_qb.queryIntent = qb.queryIntent
        return new_qb

    def process_entity_list(self, query, entities_by_type):
        for entity_type, entity_list in entities_by_type.items():
            for e in entity_list:
                if entity_type == 'CallLength':
                    e['entity'] = e['entity'].lower().replace('minutes', '').strip()
                decoder = self.get_entity_decoder(e)

                if decoder:
                    print('Decoding {}...'.format(e))
                    comparator = None
                    for comp in query.comparators:  # TODO: Move to prepare_query
                        if (comp[1]['position'] == 'before' and comp[1]['startIndex'] < e['startIndex']) \
                                or (comp[1]['position'] == 'after' and comp[1]['startIndex'] > e['startIndex']):
                            comparator = comp[0]
                    qb = decoder.decode(e, query, comparator=comparator, string_operators=query.string_operators)
                    query.merge(qb)

                elif not e["type"].startswith("_"):
                    print("No decoder for Entity", e["type"])

        if [cond for cond in query.conditions if cond[0] == 'lk' and query.choice_for_field[
            cond[1]]] and not query.string_operators and not query.logical_label:
            query.distinct_values_query = self.generateDistinctValuesQuery(query)

        if query.is_compare:
            column_counter = defaultdict(int)
            duplicated_value = None
            for cond in query.conditions:
                column_counter[cond[1]] += 1
                if column_counter[cond[1]] > 1:
                    duplicated_value = cond[1]
                    break
            conditions = query.conditions
            query.conditions = [cond for cond in conditions if cond[1] != duplicated_value]
            query.count_conditions = [cond for cond in conditions if cond[1] == duplicated_value]
            query.date_count_conditions = True
        return query

    def find_nth(self, haystack, needle, n):
        start = haystack.find(needle)
        while start >= 0 and n > 1:
            start = haystack.find(needle, start + len(needle))
            n -= 1
        return start

    def entities_normalized(self, q):
        entities = q['prediction']['entities']['$instance']
        for entity_type, entities_in_type in entities.items():
            for ix_in_type, entity in enumerate(entities_in_type):
                entity_normalized = q['prediction']['entities'][entity_type][ix_in_type]
                if isinstance(entity_normalized, list):
                    entity['entity'] = entity_normalized[0].title()
                elif isinstance(entity_normalized, dict):
                    if 'text' in entity_normalized:
                        entity['entity'] = entity_normalized['text'].title()
                    else:
                        entity['entity'] = entity_normalized
                else:
                    entity['entity'] = entity_normalized
        return entities

    def get_entities_of_types(self, types, entities):
        return [(entity, key) for key in types if key in entities for entity in entities[key]]

    def remove_duplicate_geo_entities(self, geography_entities_found, entities):
        for ix, (entity, key) in enumerate(geography_entities_found):
            for ix_, (entity_, key_) in enumerate(geography_entities_found):
                if ix == ix_:
                    continue
                if entity['text'] != entity_['text'] and entity['startIndex'] >= entity_['startIndex'] \
                        and entity['startIndex'] + entity['length'] <= entity_['startIndex'] + entity_['length']:
                    entities[key].remove(entity)
                    geography_entities_found.pop(ix)
                    break
        return geography_entities_found, entities

    def remove_dates_recognized_as_numbers(self, date_entities_found, number_entities_found, entities):
        for date_ix, (date_entity, date_key) in enumerate(date_entities_found):
            for number_ix, (number_entity, number_key) in enumerate(number_entities_found):
                if date_entity['startIndex'] + date_entity['length'] \
                        == number_entity['startIndex'] + number_entity['length']:
                    entities[number_key].remove(number_entity)
                    number_entities_found.pop(number_ix)
                    break
        return number_entities_found, entities

    def identify_geographies_to_keep(self, query):
        keep = []
        if 'county' in query.lower():
            for ix in range(query.lower().count('county')):
                keep.append(('county', self.find_nth(query.lower(), 'county', ix + 1), len('county')))
        if 'city' in query.lower():
            for ix in range(query.lower().count('city')):
                keep_ix = self.find_nth(query.lower(), 'city', ix + 1)
                keep.extend([('city', keep_ix, len('city')), ('builtin.geographyV2.city', keep_ix, len('city'))])
        if 'state' in query.lower():
            for ix in range(query.lower().count('state')):
                keep.append(
                    ('builtin.geographyV2.state', self.find_nth(query.lower(), 'state', ix + 1), len('state')))
        return keep

    def clean_overlapping_entities(self, entities):
        ix_to_ix = {}
        ix_to_type = {}
        ix_to_length = {}

        to_remove = {}
        for entity_type in entities.keys():
            to_remove[entity_type] = []

        for entity_type, entity_list in entities.items():
            for ent_ix, entity in enumerate(entity_list):
                start_index = entity['startIndex']
                found = False
                for ix in ix_to_length:
                    if ix <= start_index < (ix + ix_to_length[ix]):
                        found = True
                        if entities[ix_to_type[ix]][ix_to_ix[ix]]['type'].startswith('_') \
                                and not entity['type'].startswith('_') \
                                or 'builtin' not in entities[ix_to_type[ix]][ix_to_ix[ix]]['type'] \
                                and 'builtin' in entity['type'] and not entity_type in self.geography_entity_types:
                            to_remove[entity_type].append(ent_ix)
                        elif entity['type'].startswith('_') and not entities[ix_to_type[ix]][ix_to_ix[ix]][
                            'type'].startswith('_') \
                                or 'builtin' not in entity['type'] and 'builtin' in \
                                entities[ix_to_type[ix]][ix_to_ix[ix]]['type'] and not ix_to_type[
                                                                                           ix] in self.geography_entity_types:
                            to_remove[ix_to_type[ix]].append(ix_to_ix[ix])
                            ix_to_ix[start_index] = ent_ix
                        elif not (
                                entity['type'].startswith('_') or entities[ix_to_type[ix]][ix_to_ix[ix]][
                            'type'].startswith('_')) \
                                and ix_to_length[ix] < entity[
                            'length'] and not entity_type in self.geography_entity_types:
                            to_remove[ix_to_type[ix]].append(ix_to_ix[ix])
                            ix_to_ix[start_index] = ent_ix
                        elif not (entity['type'].startswith('_') or entities[ix_to_type[ix]][ix_to_ix[ix]][
                            'type'].startswith('_')) and not entity_type in self.geography_entity_types:
                            to_remove[entity_type].append(ent_ix)
                        break
                if not found:
                    ix_to_ix[start_index] = ent_ix
                    ix_to_type[start_index] = entity_type
                    ix_to_length[start_index] = entity['length']

        for entity_type, list_to_remove in to_remove.items():
            for ix in reversed(sorted(list(set(list_to_remove)))):
                del entities[entity_type][ix]

        return entities

    def clean_geography_ents(self, geography_entities_found, keep, entities):
        geo_by_type = defaultdict(list)
        geo_ents = []
        for entity, entity_type in geography_entities_found:

            # If a keep matches on index
            #   if matches on type, keep entity
            #   else skip
            # else keep

            skip = False
            for value, ix, length in keep:
                if abs((entity['startIndex'] + entity['length']) - ix) <= 2 or abs(
                        entity['startIndex'] - (ix + length)) <= 4:  # keep matches on index
                    if value.lower() == entity['type'].lower():
                        skip = False
                        break
                    else:
                        skip = True
            if not skip:
                if entity_type in entities and entity in entities[entity_type]:
                    entities[entity_type].remove(entity)
                if entity['text'] in geo_by_type[self.geo_transformations[entity['type']]]:
                    continue
                else:
                    geo_ents.append((entity, entity_type))
                    geo_by_type[self.geo_transformations[entity['type']]].append(entity['text'])
                    entity['type'] = self.geo_transformations[entity['type']].title()
            elif entity_type in self.geography_entity_types and entity_type in entities:
                entities[entity_type].remove(entity)
        return entities, geo_ents, geo_by_type

    def generate_entity_permutations(self, geo_ents, entities):
        text_to_entity = defaultdict(list)
        for ent, ent_type in geo_ents:
            text_to_entity[ent['text']].append((ent, ent_type))
        geo_perms = [list(perm) for perm in list(itertools.product(*[e for e in text_to_entity.values()]))]
        lists = []
        for perm in geo_perms:
            entities_perm = copy.deepcopy(entities)
            for ent, ent_type in perm:
                entities_perm[ent_type].append(ent)
            lists.append(entities_perm)
        return lists

    def prioritize_field_names(self, entities):
        _field_names, _field_name_entities = findFieldNames(entities)

        # priority for fieldnames except for _DataElements

        for _fieldName in _field_name_entities:
            start_index = _fieldName['startIndex']
            end_index = _fieldName['startIndex'] + _fieldName['length']
            ixs_to_remove = []
            for ix, entity in enumerate(entities):
                if entity == _fieldName or entity['type'] == '_DataElement':
                    continue
                entity_start_index = entity['startIndex']
                entity_end_index = entity['startIndex'] + entity['length']
                if entity_start_index >= start_index and entity_end_index <= end_index:
                    ixs_to_remove.append(ix)
            for ix in reversed(ixs_to_remove):
                entities.pop(ix)

        return entities

    def handle_filters(self, q, filter_entities):
        query = q['query'].lower()
        if filter_entities:
            for filter_, _ in filter_entities:
                query = query.replace(filter_['text'].lower(), filter_['entity'].lower())
            q = interpret(query,
                          self.luis_config['luis_app_id'],
                          self.luis_config["luis_subscription_key"])
            del q['prediction']['entities']['_Filter']
            del q['prediction']['entities']['$instance']['_Filter']
        entities = self.entities_normalized(q)
        entities = self.clean_overlapping_entities(entities)
        return q, entities

    def prepare_query(self, q, prev_q, query_processor, is_a_prev_query=False):
        self.luis = q

        union = False

        # First setup the context for the intent assigned by LUIS
        # this_intent = q["topScoringIntent"]["intent"]
        this_intent = q["prediction"]["topIntent"]
        intent_decoder = self.get_intent_decoder(this_intent)
        if not intent_decoder:
            print("Unable to continue. Un-recognized intent: ", this_intent)
            return False

        entities = self.entities_normalized(q)
        entities = self.clean_overlapping_entities(entities)

        filter_entities = self.get_entities_of_types(['_Filter'], entities)
        q, entities = self.handle_filters(q, filter_entities)

        geography_entities_found = self.get_entities_of_types(self.geography_entity_types, entities)
        geography_entities_found, entities = self.remove_duplicate_geo_entities(geography_entities_found, entities)

        date_entities_found = self.get_entities_of_types(self.date_entity_types, entities)

        number_entities_found = self.get_entities_of_types(self.number_entity_types, entities)
        number_entities_found, entities = self.remove_dates_recognized_as_numbers(date_entities_found,
                                                                                  number_entities_found, entities)

        print('There are {} geography entities.'.format(
            len(geography_entities_found)))
        pprint(geography_entities_found)

        queries = []
        supplementary_queries = []

        keep = self.identify_geographies_to_keep(q['query'])
        entities_no_geo, geo_ents, geo_by_type = self.clean_geography_ents(geography_entities_found, keep, entities)

        lists = self.generate_entity_permutations(geo_ents, entities_no_geo)

        for lst in lists:
            flat_entity_list = [entity for entity_type in lst for entity in lst[entity_type]]
            flat_entity_list = self.prioritize_field_names(flat_entity_list)
            entity_list, query = intent_decoder.decode(
                this_intent, flat_entity_list, prev_q=prev_q, is_a_prev_query=is_a_prev_query)
            query = self.process_entity_list(query, lst)
            queries.append(query)

        return queries, union, supplementary_queries
