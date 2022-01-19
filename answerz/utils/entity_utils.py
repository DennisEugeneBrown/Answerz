def findEntityByType(entities, type_name, return_entity=False, return_many=False):
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


def findFieldNames(entities):
    field_names = []
    field_name_entities = []
    for e in entities:
        if e["type"] == '_FieldName':
            field_names.append(e['entity'])
            field_name_entities.append(e)
    return field_names, field_name_entities


def handle_number_entities(entities, mapped_element):
    _field_names, _field_name_entities = findFieldNames(entities)
    _element_ix, _element = findEntityByType(entities, "_DataElement")

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

    return entities


def handle_groupings(mapped_groupings, qb):
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
    return qb
