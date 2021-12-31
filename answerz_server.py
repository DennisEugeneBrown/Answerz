import us
import csv
import copy
import json
import pyodbc
import pprint
import requests
import itertools
import pandas as pd
from pprint import pprint
from datetime import datetime
from collections import defaultdict


class MSSQLReader:
    def __init__(self, db_config):
        self.connection = None
        self.server = db_config

    def connect(self, server):
        connection_string = 'Driver={};Server={};Database={};Uid={};Pwd={};Port={};'.format(
            self.server["driver"], self.server["host"], self.server["database"], server["user"], server["password"],
            server["port"])
        self.connection = pyodbc.connect(connection_string)
        return self.connection

    def query(self, query):
        cursor = self.connection.cursor()
        print(query)
        cursor.execute(query)
        return cursor


class DataMapRepo:
    def __init__(self, data_map):
        self.DATA_MAP = data_map

    def getAllGroupings(self, _element):
        mapped_element = None
        _element = _element.lower()
        if not _element in self.DATA_MAP:
            print("FATAL ERROR. Missing mapping for _DataElement = ", _element)
            return
        else:
            mapped_element = self.DATA_MAP[_element]

        mapped_groupings = []
        for group in mapped_element["Groupings"]:
            mapped_groupings.append(group)
        return mapped_groupings

    def findGrouping(self, _element, _groupAction):
        mapped_element = None
        _element = _element.lower()
        if _element not in self.DATA_MAP:
            print("FATAL ERROR. Missing mapping for _DataElement = ", _element)
            return
        else:
            mapped_element = self.DATA_MAP[_element]

        print('Group Action: ', _groupAction)
        mapped_grouping = None
        for group in mapped_element["Groupings"]:
            if (group["name"] == _groupAction):
                mapped_grouping = group
                break
        if not mapped_grouping:
            for group in mapped_element["Groupings"]:
                if not "default" in group:
                    continue

                if (group["default"] == True):
                    mapped_grouping = group
                    break

        return mapped_element, mapped_grouping

    def findMapping(self, _element, _aggregation):

        mapped_element = None
        _element = _element.lower()
        if not _element in self.DATA_MAP:
            print("FATAL ERROR. Missing mapping for _DataElement = ", _element, "... Using Calls.")
            # return
            mapped_element = self.DATA_MAP['Calls']
        else:
            mapped_element = self.DATA_MAP[_element]

        mapped_aggregation = None
        for agg in mapped_element["Aggregations"]:
            if (agg["name"] == _aggregation):
                mapped_aggregation = agg
                break

        if not mapped_aggregation:
            for agg in mapped_element["Aggregations"]:
                if "default" not in agg:
                    continue

                if agg["default"]:
                    mapped_aggregation = agg
                    break

        return mapped_element, mapped_aggregation


class QueryBlock:
    def __init__(self, intent_summary=None):
        self.queryIntent = intent_summary
        self.tables = []
        self.selects = []
        self.joins = []
        self.conditions = []
        self.count_conditions = []
        self.date_count_conditions = False
        self.groups = []
        self.sorts = []
        self.comparators = []
        self.string_operators = []
        self.is_compare = False
        self.is_cross = False
        self.aggregation = None
        self.unions = []
        self.distinct_values_query = None
        self.with_query = None
        self.totals = None
        self.is_total = False
        self.unpivot_selects = []
        self.unpivot_cols = []
        self.groups_to_skip = []
        self.cond_sep = {-1: 'AND'}  # Default
        self.logical_label = False
        self.condition_locs = {}
        self.conditions_by_type = {}

    def addTable(self, tableNames, join=None):
        if not isinstance(tableNames, list):
            tableNames = [tableNames]
        for tableName in tableNames:
            if join:
                for join in self.joins:
                    if (join[0] == tableName):
                        #
                        # Already have this join. Let's hope it doesnt conflict!
                        #
                        break

                self.joins.append((tableName, join))
            else:
                self.tables.append(tableName)

    def addGroup(self, group_field):
        self.groups.append(group_field)

    def generateConditionalCountSelect(self, conditions):
        # Count(IIF(CallLog3.City = 'Anaheim', 1, null)) AS [(CallLog3.City = 'Anaheim')],
        qbr = QueryBlockRenderer()
        rendered_condition = qbr.renderConditions(conditions)
        sql = "Count(IIF({}, 1, null))".format(rendered_condition)
        # field_name = rendered_condition if len(rendered_condition) < 125 else ' AND '.join(
        #     [cond[0][-1] for cond in conditions.values()])
        field_name = ' And '.join([cond[0][-1].replace('%', '') for cond in conditions.values()])
        return [sql, field_name]

    def getAllSelects(self):
        # the order here is intentionally backwards. we pre-end the select
        # This is a way to ensure the array is COPIED and not REFERENCED. as we will be modifying the array
        allSelects = [group for group in self.groups[:] if
                      group[0] not in self.groups_to_skip and group not in self.selects]
        allSelects.extend(
            [select for select in self.selects if select[0] not in self.groups_to_skip])

        if self.conditions_by_type:
            pivot_condition_type = list(self.conditions_by_type.keys())[-1]
            pivot_conditions_count = len(self.conditions_by_type[pivot_condition_type])
            other_condition_types = list(self.conditions_by_type.keys())[:-1]
            for condition in self.conditions_by_type[pivot_condition_type]:
                other_condition_selects = []
                for other_condition_type in other_condition_types:
                    if len(self.conditions_by_type[other_condition_type]) <= 1:
                        continue
                    for other_condition in self.conditions_by_type[other_condition_type]:
                        other_condition_selects.append(self.generateConditionalCountSelect(
                            {pivot_condition_type: [condition], other_condition_type: [other_condition]}))
                if other_condition_selects:
                    allSelects.extend(other_condition_selects)
                if pivot_conditions_count > 1:
                    allSelects.append(self.generateConditionalCountSelect({pivot_condition_type: [condition]}))

        return allSelects

    def merge_conditions_by_type(self, other_conditions):
        conditions_by_type = defaultdict(list)
        for cond_type, vals in self.conditions_by_type.items():
            conditions_by_type[cond_type].extend(vals)
        for cond_type, vals in other_conditions.items():
            conditions_by_type[cond_type].extend(vals)
        return conditions_by_type

    def merge(self, qb_other):
        if (not qb_other):
            return False
        if (qb_other.tables and qb_other.tables != self.tables):
            print("Root table mismatch on Query Block merge")
            print(qb_other.tables)
            print(self.tables)
            print("---")
            return

        # This is naive for now
        self.selects.extend(qb_other.selects)
        self.joins.extend(tuple(qb_other.joins))
        self.conditions.extend(qb_other.conditions)
        self.count_conditions.extend(qb_other.count_conditions)
        self.groups.extend(qb_other.groups)
        self.sorts.extend(qb_other.sorts)
        self.comparators.extend(qb_other.comparators)
        self.is_compare = (self.is_compare or qb_other.is_compare)
        self.aggregation = qb_other.aggregation if qb_other.aggregation else self.aggregation
        self.logical_label = qb_other.logical_label
        self.condition_locs = {**self.condition_locs, **qb_other.condition_locs}
        self.conditions_by_type = self.merge_conditions_by_type(qb_other.conditions_by_type)
        return True


# this is a naive renderer with no schema validation
class QueryBlockRenderer:
    def render(self, qb):
        sql = ""

        if qb.count_conditions:
            qb.selects = self.processCountConditions(qb,
                                                     agg=qb.queryIntent[1].upper() if qb.queryIntent[1] else 'COUNT')

        cond_sql = self.renderConditionsInQuery(qb)
        if cond_sql and len(cond_sql) <= 128:
            for ix, select in enumerate(qb.selects):
                if 'Count_' in select[1]:
                    for sort_ix, sort in enumerate(qb.sorts):
                        if select[1] == sort[0]:
                            qb.sorts[sort_ix] = ('[' + cond_sql + ']', sort[1])
                    qb.selects[ix][1] = cond_sql

            qb.selects[0][1] = cond_sql

        if qb.with_query and qb.conditions:
            qb.with_query.selects = [['COUNT(*)', 'Total_Responses'], ["SUM({})".format(
                ' * '.join(["IIF({} != '', 1, 0)".format(cond[1]) for cond in qb.conditions])), 'Valid_Responses']]
        add_with_table = False
        if (not qb.groups and qb.with_query and qb.conditions) or qb.is_total:
            add_with_table = True
            if 'total' not in qb.tables:
                qb.addTable('total')
            if not qb.is_total:
                # qb.groups.append([qb.with_query.selects[0][-1], qb.with_query.selects[0][-1]])
                for select in qb.with_query.selects:
                    qb.selects.append([select[-1], select[-1]])
                    qb.groups.append([select[-1], select[-1]])
                qb.groups_to_skip.append(qb.with_query.selects[0][-1])
                qb.selects.append(
                    ["CAST(CAST(COUNT(*) * 100.0 / {} AS decimal(10, 2)) AS varchar) + '%'".format(
                        qb.with_query.selects[0][-1]),
                        'Percentage'.format(qb.with_query.selects[-1][-1])])

            if not qb.is_total:
                qb.totals = QueryBlock()
                qb.totals.is_total = True
                qb.totals.addTable(qb.tables)
                qb.totals.with_query = QueryBlock()
                qb.totals.with_query.selects = [[qb.selects[0][0], 'Total_Records']]
                qb.totals.with_query.tables = qb.tables[0:1]
                qb.totals.with_query.joins = qb.joins
                qb.totals.groups.append(['Total_Records', 'Total_Records'])
                if qb.conditions:
                    qb.totals.with_query.selects.append(["SUM({})".format(
                        ' * '.join(["IIF({} != '', 1, 0)".format(cond[1]) for cond in qb.conditions])),
                        'Valid_Responses'])
                    qb.totals.with_query.selects.append(["SUM({})".format(
                        ' * '.join(["IIF({} = '', 1, 0)".format(cond[1]) for cond in qb.conditions])),
                        'Blanks_or_Nulls'])
                    qb.totals.groups.append(['Valid_Responses', 'Valid_Responses'])
                    qb.totals.groups.append(['Blanks_or_Nulls', 'Blanks_or_Nulls'])
                qb.totals.unpivot_selects = [['col', 'col'], ['value', 'value'],
                                             [
                                                 "cast(cast(value * 100.0 / [Total_Records] as decimal(10, 2)) as varchar) + '%'",
                                                 'percentage']]
                qb.totals.unpivot_cols = [col[0] for col in qb.totals.groups]

        sql = sql + "\nSELECT\n\t" + self.renderSelect(qb)
        sql = sql + "\nFROM\n\t" + self.renderFrom(qb)

        if (cond_sql):
            sql = sql + "\nWHERE " + cond_sql

        group_sql = self.renderGroups(qb)
        if (group_sql):
            sql = sql + "\nGROUP BY " + group_sql

        if qb.unions and len(qb.groups) < 2:
            qbr = QueryBlockRenderer()
            qb2 = qbr.render(qb.unions[0])
            sql += ' UNION ' + qb2

        order_sql = self.renderSorts(qb)
        if (order_sql):
            sql = sql + "\nORDER BY " + order_sql

        if qb.unpivot_cols:
            sql = self.renderUnpivot(qb, sql)
            print('sql is')

        if add_with_table:
            qbr = QueryBlockRenderer()
            sql = 'WITH total AS({})'.format(qbr.render(qb.with_query)) + sql

        return sql

    def renderUnpivotSelect(self, qb):
        sep = ""
        sql = ""
        for term in qb.unpivot_selects:
            sql = sql + sep + term[0] + " AS [" + term[1] + "]"
            sep = ", "
        return sql

    def renderUnpivot(self, qb, sql):
        sql = 'SELECT ' + self.renderUnpivotSelect(qb) + ' FROM ({})'.format(sql)
        sql = sql + 'AS SourceTable UNPIVOT (value for col in ({})) AS PivotTable, total;'.format(
            ','.join(qb.unpivot_cols))
        return sql

    def renderSelect(self, qb):
        sep = ""
        sql = ""

        # Handle the group selects
        for term in qb.getAllSelects():
            sql = sql + sep + term[0] + " AS [" + term[1] + "]"
            sep = ", "

        return sql

    def renderFrom(self, qb, add_with_table=False):
        sql = ''
        for table in qb.tables:
            sql += table
            # if add_with_table:
            #     sql += ', total'
            if (len(qb.joins)):
                for join in set([tuple(join) for join in qb.joins]):
                    if table in join[1].split('.'):
                        sql = sql + "\n\tJOIN " + join[0] + " ON " + join[1]
            sql += ', '
        return sql.strip().rstrip(',')

    def renderGroups(self, qb):
        sep = ""
        sql = ""

        # Handle the group selects
        for term in qb.groups:
            sql = sql + sep + term[0]
            sep = ", "

        return sql

    def renderSorts(self, qb):
        sep = ""
        sql = ""

        # Handle the group selects
        for term in qb.sorts:
            if term[1] == 'ASC':
                sql = sql + sep + \
                      "case when ({value} is null or {value} like '') then 1 else 0 end, {value} ASC".format(
                          value=term[0])
            else:
                sql = sql + sep + ' '.join(term)
            sep = ", "

        return sql

    def renderConditionsInQuery(self, qb):
        return self.renderConditions(qb.conditions_by_type)

    def renderConditions(self, conditions):
        sql = ""

        def encodeLHS(lhs):

            return lhs

        def encodeRHS(rhs):
            return "'" + str(rhs) + "'"

        def encodeCondition(cond):
            op, lhs, rhs = cond

            if (op == "eq"):
                return encodeLHS(lhs) + " = " + encodeRHS(rhs)
            if (op == "lk"):
                return encodeLHS(lhs) + " like " + encodeRHS(rhs)
            if (op == "lt"):
                return encodeLHS(lhs) + " < " + encodeRHS(rhs)
            if (op == "lte"):
                return encodeLHS(lhs) + " <= " + encodeRHS(rhs)
            if (op == "gt"):
                return encodeLHS(lhs) + " > " + encodeRHS(rhs)
            if (op == "gte"):
                return encodeLHS(lhs) + " >= " + encodeRHS(rhs)
            if (op == "not"):
                return encodeLHS(lhs) + " != " + encodeRHS(rhs)
            return op

        sql = sql + ' AND '.join(
            ['(' + ' OR '.join([encodeCondition(cond) for cond in conds]) + ')' for conds in
             conditions.values()])

        return sql

    def processCountConditions(self, qb, agg='COUNT'):
        selects = []

        def encodeSelect(lhs, rhs, agg, encoded_op, and_=None):
            if agg.lower() == 'avg':
                field = "CAST(dbo.{}.{} AS {})".format(qb.tables[0], 'CallLength', 'INT')
            else:
                field = 1

            selects.append(
                ["{agg}(IIF({lhs} {encoded_op} '{rhs}'{and_}, {field}, NULL))".format(lhs=lhs, rhs=rhs, agg=agg,
                                                                                      encoded_op=encoded_op,
                                                                                      field=field,
                                                                                      and_="AND {lhs} {encoded_op} '{rhs}'".format(
                                                                                          lhs=and_[0], rhs=and_[1],
                                                                                          encoded_op=and_[
                                                                                              2]) if and_ else ''),
                 '{agg}_{rhs}'.format(agg=agg, rhs=rhs if rhs else 'NULL')])

            selects.append([
                "CONCAT(IIF({agg}(*)>0,{agg}(IIF({lhs} {encoded_op} '{rhs}'{and_}, {field}, NULL)) * 100 / {agg}(*), 0), '%')".format(
                    lhs=lhs, rhs=rhs, agg=agg,
                    encoded_op=encoded_op,
                    field=field, and_="AND {lhs} {encoded_op} '{rhs}'".format(lhs=and_[0], rhs=and_[1],
                                                                              encoded_op=and_[2]) if and_ else ''),
                '{agg}_{rhs}_PERCENT'.format(agg=agg, rhs=rhs if rhs else 'NULL')])

        def encodeCondition(cond, agg):
            if len(cond) == 2:
                conds = []
                for cond_ in cond:
                    op, lhs, rhs = cond_
                    encoded_op = " = "
                    if (op == "eq"):
                        encoded_op = " = "
                    if (op == "lk"):
                        encoded_op = " like "
                    if (op == "lt"):
                        encoded_op = " < "
                    if (op == "lte"):
                        encoded_op = " <= "
                    if (op == "gt"):
                        encoded_op = " > "
                    if (op == "gte"):
                        encoded_op = " >= "
                    conds.append([lhs, rhs, encoded_op])
                encodeSelect(conds[0][0], conds[0][1], agg, conds[0][2], and_=[conds[1][0], conds[1][1], conds[1][2]])

            else:
                op, lhs, rhs = cond
                encoded_op = " = "
                if (op == "eq"):
                    encoded_op = " = "
                if (op == "lk"):
                    encoded_op = " like "
                if (op == "lt"):
                    encoded_op = " < "
                if (op == "lte"):
                    encoded_op = " <= "
                if (op == "gt"):
                    encoded_op = " > "
                if (op == "gte"):
                    encoded_op = " >= "
                encodeSelect(lhs, rhs, agg, encoded_op)

        other_selects = set()
        for term in qb.conditions:
            other_selects.add(tuple(term))
        for term in other_selects:
            selects.append(["'" + str(term[2]) + "'", term[1]])

        # Handle the group selects
        if qb.date_count_conditions and len(qb.count_conditions) == 4:
            encodeCondition([qb.count_conditions[0], qb.count_conditions[1]], agg)
            encodeCondition([qb.count_conditions[2], qb.count_conditions[3]], agg)
        else:
            for term in qb.count_conditions:
                encodeCondition(term, agg)

        return selects


class QueryTestBlocks():
    # This is defined as a class level function not an instance level function. its a way to manage globals
    def simple_count(self):
        qb = QueryBlock()
        qb.addTable("CallLog")
        qb.selects.append(("Count(distinct CallReportNum)", "Measure"))
        return qb

    # This is defined as a class level function not an instance level function. its a way to manage globals
    def simple_count_with_date_filter(self):
        qb = QueryBlock()
        qb.addTable("CallLog")
        qb.selects.append(("Count(distinct CallReportNum)", "Measure"))

        qb.conditions.append(["gte", "CallLog.Date", "2018-01-01"])
        qb.conditions.append(["lt", "CallLog.Date", "2019-01-01"])

        return qb

    # This is defined as a class level function not an instance level function. its a way to manage globals
    def simple_count_and_join(self):
        qb = QueryBlock()
        qb.addTable("CallLog")
        qb.selects.append(("Count(distinct CallReportNum)", "Measure"))
        qb.joins.append(("CallLogNeeds", "CallLog.CallReportNum=CallLogNeeds.CallLogId"))
        return qb

    # This is defined as a class level function not an instance level function. its a way to manage globals
    def simple_count_and_grouped_join(self):
        qb = QueryBlock()
        qb.addTable("CallLog")
        # qb.selects.append(("Count(distinct CallReportNum)", "Measure"))
        qb.addTable("CallLog")
        qb.addTable("CallLogNeeds",
                    "CallLog.CallReportNum=CallLogNeeds.CallLogId")
        # qb.joins.append(("CallLogNeeds", "CallLog.CallReportNum=CallLogNeeds.CallLogId"))
        qb.groups.append(["CallLogNeeds.Need", "Need"])
        return qb

    def chained_select_and_group(self):
        qb1 = QueryBlock()
        qb1.tables = ["CallLog"]
        qb1.selects.append(("Count(distinct CallReportNum)", "Measure"))

        qb2 = QueryBlock()
        qb2.tables = ["CallLog"]
        qb2.joins.append(
            ("CallLogNeeds", "CallLog.CallReportNum=CallLogNeeds.CallLogId"))
        qb3 = QueryBlock()
        qb3.groups.append(["CallLogNeeds.Need", "Need"])

        qb = QueryBlock()
        qb.merge(qb1)
        qb.merge(qb2)
        return qb


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
        fieldNames = []
        fieldName_entities = []
        for e in entities:
            if (e["type"] == '_FieldName'):
                fieldNames.append(e['entity'])
                fieldName_entities.append(e)
        return fieldNames, fieldName_entities

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

        _fieldNames, _fieldName_entities = self.findFieldNames(entities)

        # priority for fieldnames always

        for _fieldName in _fieldName_entities:
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
        for ix, field_name in enumerate(_fieldNames):
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
                    _fieldName_entity = _fieldName_entities[num_entity_ix]
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
                    # mapped_groupings = data_map_repo.getAllGroupings(_element)
                    pass
            elif _groupAction['entity'].lower() == 'compare':
                qb.is_compare = True
        elif not _groupAction and not is_a_prev_query:
            _, mapped_grouping = data_map_repo.findGrouping(
                _element, 'Year')
            mapped_groupings.append(mapped_grouping)
            _groupAction = {'entity': 'group by'}
            default_grouping = True

        for table in mapped_aggregation["tables"]:
            if (type(table) == str):
                qb.addTable(table)
            else:
                qb.addTable(table[0], table[1])

        for col in mapped_aggregation["columns"]:
            if ("type" in col and col["type"] == "agg"):
                if (col["agg"] == "count"):
                    if ("field" in col and col["field"]):
                        if (col["distinct"]):
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
                elif (col['agg'] == 'avg'):
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
                    _fieldNames or _logicalLabel or default_grouping):
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
                # qb2 = QueryBlock()
                # qb2.selects = [["'Total'", qb.groups[0][1]],
                #                ['sum({}) over ()'.format(qb.selects[0][0]), qb.selects[0][1]],
                #                ["'100%'", qb.selects[1][1]]]
                #
                # qb2.queryIntent = qb.queryIntent
                # qb2.tables = qb.tables
                # qb.unions.append(qb2)
        # else:

        # qb.addTable("CallLog")

        return entities, qb


def findFieldNames(entities):
    fieldNames = []
    for e in entities:
        if (e["type"] == '_FieldName'):
            fieldNames.append(e['entity'])
    return fieldNames


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
            if (e["type"] == '_FieldName'):
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
            if (type(table) == str):
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
            if (type(table) == str):
                qb.addTable(table)
            else:
                qb.addTable(table[0], table[1])

        for col in mapped_aggregation["columns"]:
            if ("type" in col and col["type"] == "agg"):
                if (col["agg"] == "count"):
                    if ("field" in col and col["field"]):
                        if (col["distinct"]):
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
            if (e["type"] == '_FieldName'):
                fieldNames.append(e['entity'])
                fieldName_entities.append(e)
        return fieldNames, fieldName_entities

    # We pass the entire list of entities to the decoder although we expect most to be ignored here
    def decode(self, intent_name, entities, prev_q=None, is_a_prev_query=False):
        # global DATA_MAP

        qb = prev_q
        try:
            _element = qb.queryIntent[0]  # TODO: Add and use a getter
            _aggregation = qb.queryIntent[1]  # TODO: Add and use a getter
        except:
            print('ERROR. CANNOT FIND PREVIOUS QUERY TO BREAKDOWN.')
            return QueryBlock()

        _groupAction_ix, _groupAction = self.findEntityByType(entities, "_GroupAction", return_entity=True)
        # _fieldName_ix, _fieldName = self.findEntityByType(entities, "_FieldName")
        _logicalLabel_ix, _logicalLabel = self.findEntityByType(entities, "_LogicalLabel")

        _fieldNames, _fieldName_entities = self.findFieldNames(entities)

        for _fieldName in _fieldName_entities:
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
        _, mapped_aggregation = data_map_repo.findMapping(
            _element, _aggregation)

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
                    # mapped_groupings = data_map_repo.getAllGroupings(_element)
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
                'percentage'])
            # qb2 = QueryBlock()
            # qb2.selects = [["'Total'", qb.groups[0][1]],
            #                ['sum({}) over ()'.format(qb.selects[0][0]), qb.selects[0][1]],
            #                ["'100%'", qb.selects[1][1]]]
            # qb2.queryIntent = qb.queryIntent
            # qb2.tables = qb.tables
            # qb.unions.append(qb2)

        return entities, qb


class EntityDecoderBase:
    def lookupTablesAndField(self, _element, _aggregation, entity_name, data_map):
        self.data_map = data_map

        data_map_repo = DataMapRepo(data_map)
        mapped_element, mapped_aggregation = data_map_repo.findMapping(
            _element, _aggregation)
        # print('LOOKING FOR {}'.format(entity_name))
        for dim in mapped_element["Dimensions"]:
            if (dim["name"] == entity_name):
                tables = mapped_aggregation["tables"]
                # No handling yet for additional table requirements on field

                result = dim
                result["tables"] = tables
                return result

        return None

    def lookupTablesAndFieldByType(self, _element, _aggregation, entity_type, data_map):
        self.data_map = data_map

        data_map_repo = DataMapRepo(self.data_map)
        mapped_element, mapped_aggregation = data_map_repo.findMapping(
            _element, _aggregation)
        for dim in mapped_element["Dimensions"]:
            if (dim["type"] == entity_type):
                tables = mapped_aggregation["tables"]
                # No handling yet for additional table requirements on field

                result = dim
                result["tables"] = tables
                return result

        return None


class ColumnEntityDecoder(EntityDecoderBase):
    def __init__(self, data_map):
        self.data_map = data_map
        pass

    def mapValues(self, table, field_name, entity_value):
        return [entity_value]

    # Takes the entity to decode + a potential query_block to augment
    def decode(self, entity, query_block, comparator=None, string_operators=None):
        # print(entity)
        if 'resolution' in entity:
            if 'values' in entity['resolution']:
                values = entity["resolution"]["values"]
            else:
                values = [entity['resolution']['value']]
        else:
            values = [entity['entity']]

        if (len(values) == 1):

            entity_name = entity["type"]
            entity_value = values[0]

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
                state_name = entity_value['value'].replace(
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
            if lu and 'exact_match' in lu and lu['exact_match'] == False:
                exact_match = False

            qb = QueryBlock(query_block.queryIntent)

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

        if (len(values) == 1):

            entity_name = entity["type"]
            if 'resolution' in entity:
                entity_value = entity['entity']
            else:
                entity_value = entity['entity']

            lu = self.lookupTablesAndField(
                query_block.queryIntent[0], query_block.queryIntent[1], entity_name, self.data_map)

            tables = lu["tables"]
            field_name = lu['field'][entity_value]

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
            if 'exact_match' in lu and lu['exact_match'] == False:
                exact_match = False

            qb = QueryBlock(query_block.queryIntent)
            qb.logical_label = True

            for table in tables:
                if (type(table) == str):
                    qb.addTable(table)
                else:
                    # note: this is not yet tested and may break
                    qb.addTable(table[0], table[1])

            db_values = self.mapValues(tables[0], field_name, entity_value)
            if (len(db_values) == 1):
                if string_operators:
                    if ("." in field_name):  # already scoped
                        qb.conditions.append(
                            [(string_operators[0].format(field_name, entity_value)), '', ''])
                    else:
                        qb.conditions.append(
                            [(string_operators[0].format(tables[0] + "." + field_name, entity_value)), '', ''])
                elif exact_match:
                    if ("." in field_name):  # already scoped
                        qb.conditions.append(["eq", field_name, entity_value])
                    else:
                        qb.conditions.append(
                            ["eq", tables[0] + "." + field_name, entity_value])
                else:
                    if ("." in field_name):  # already scoped
                        qb.conditions.append(
                            ["lk", field_name, '%' + entity_value + '%'])
                    else:
                        qb.conditions.append(
                            ["lk", tables[0] + "." + field_name, '%' + entity_value + '%'])

                if ("joins" in lu and lu["joins"]):
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


class DateRangeEntityDecoder(EntityDecoderBase):
    def __init__(self, data_map):
        self.data_map = data_map

    # Takes the entity to decode + a potential query_block to augment
    def decode(self, entity, query_block=None, comparator=None, string_operators=None):
        value = entity['entity']

        if (value["type"] == "daterange"):

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
                qb.conditions.append(
                    ["gte", field_name, entity_value["start"]])
            if "end" in entity_value:
                qb.conditions.append(
                    ["lt", field_name, entity_value["end"]])
            else:
                qb.conditions.append(
                    ["lt", field_name, datetime.now().strftime('%Y-%m-%d')])

            if ("joins" in lu and lu["joins"]):
                for join in lu["joins"]:
                    qb.addTable(join[0], join[1])

            return qb

        return None


class DateEntityDecoder(EntityDecoderBase):
    def __init__(self, data_map):
        self.data_map = data_map

    # Takes the entity to decode + a potential query_block to augment
    def decode(self, entity, query_block=None, comparator=None, string_operators=None):
        values = entity["resolution"]["values"]

        if (len(values) == 1):

            value = values[0]
            if (value["type"] == "date"):

                entity_type = "datetime"
                entity_value = entity['entity']

                lu = self.lookupTablesAndFieldByType(
                    query_block.queryIntent[0], query_block.queryIntent[1], entity_type, self.data_map)
                if lu:
                    tables = lu["tables"]
                    field_name = lu["field"]
                    display_name = lu["display_name"] if "display_name" in lu else ''
                else:
                    tables = []
                    field_name = ''
                    display_name = ''

                qb = QueryBlock(query_block.queryIntent)

                qb.conditions.append(
                    ["gte", field_name, entity_value["start"]])
                if "end" in entity_value:
                    qb.conditions.append(
                        ["lt", field_name, entity_value["end"]])
                else:
                    qb.conditions.append(
                        ["lt", field_name, datetime.now().strftime('%Y-%m-%d')])

                if ("joins" in lu and lu["joins"]):
                    for join in lu["joins"]:
                        qb.addTable(join[0], join[1])

                return qb

        return None


class LuisIntentProcessor:

    def __init__(self, data_map):
        # Intent Decoders
        self.data_map = data_map
        self.i_decoders = {}
        self.i_decoders["agg-elements-by-description"] = AggregationByDescriptionIntentDecoder(
            data_map)
        self.i_decoders["agg-elements-by-logical-yes"] = AggregationByLogicalYesDecoder(
            data_map)
        self.i_decoders["breakdown-by"] = BreakdownByIntentDecoder(
            data_map)
        self.i_decoders["cross-by-field-name"] = CrossByFieldNameIntentDecoder(
            data_map)

        # Entity Decoders
        self.e_decoder_default = ColumnEntityDecoder(data_map)
        self.e_decoders = {}
        self.e_decoders["builtin.datetimeV2.daterange"] = DateRangeEntityDecoder(
            data_map)
        self.e_decoders["builtin.datetimeV2.date"] = DateEntityDecoder(
            data_map)
        self.e_decoders["_LogicalLabel"] = LogicalLabelEntityDecoder(
            data_map)

        self.geo_transformations = {
            'County': 'county',
            'builtin.geographyV2.state': 'state',
            'State': 'state',
            'builtin.geographyV2.city': 'city',
            'City': 'city',
        }
        self.geography_entity_types = ['geographyV2', 'County', 'City']
        self.date_entity_types = ['builtin.datetimeV2', 'builtin.datetimeV2.daterange']  # TODO: test dates
        self.number_entity_types = ['builtin.number']  # TODO: test dates

    def get_intent_decoder(self, intent_name):

        if (intent_name in self.i_decoders):
            return self.i_decoders[intent_name]

        return None

    def get_field_values(self, field, table, query_processor):
        return query_processor.run_query('SELECT DISTINCT {} FROM {}'.format(field, table), [[field, field]])

    def get_entity_decoder(self, entity):

        t = entity["type"]
        if (t in self.e_decoders):
            return self.e_decoders[t]

        if (t.startswith("_")):
            # this is a system field
            return None

        return self.e_decoder_default

    def generateDistinctValuesQuery(self, qb):
        new_qb = QueryBlock()
        new_qb.tables = qb.tables[0:1]
        new_qb.conditions = [cond for cond in qb.conditions if cond[0] == 'lk' or ' like ' in cond[0]]
        new_qb.conditions_by_type = {}
        for type_, conds in qb.conditions_by_type.items():
            for cond in conds:
                if cond[0] == 'lk' or ' like ' in cond[0]:
                    if type_ in new_qb.conditions_by_type:
                        new_qb.conditions_by_type[type_].append(cond)
                    else:
                        new_qb.conditions_by_type[type_] = [cond]
        field_name = new_qb.conditions[0][1] if new_qb.conditions[0][1] else new_qb.conditions[0][0].split(' like ')[0]
        new_qb.selects = [['COUNT(*)', field_name]]
        new_qb.groups = [[field_name, field_name]]
        new_qb.joins = qb.joins
        return new_qb

    def process_entity_list(self, entity_list, query, entities_by_type):
        for entity_type, entity_list in entities_by_type.items():
            for e in entity_list:
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

        if [cond for cond in query.conditions if
            cond[0] == 'lk'] and not query.string_operators and not query.logical_label:
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
                # entity_normalized = entities[entity_type][ix_in_type]
                if isinstance(entity_normalized, list):
                    entity['entity'] = entity_normalized[0]
                elif isinstance(entity_normalized, dict):
                    if 'text' in entity_normalized:
                        entity['entity'] = entity_normalized['text']
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
                                and 'builtin' in entity['type']:
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
                                and ix_to_length[ix] < entity['length']:
                            to_remove[ix_to_type[ix]].append(ix_to_ix[ix])
                            ix_to_ix[start_index] = ent_ix
                        elif not (entity['type'].startswith('_') or entities[ix_to_type[ix]][ix_to_ix[ix]][
                            'type'].startswith('_')) and not entity['type'] in self.geography_entity_types:
                            to_remove[entity_type].append(ent_ix)
                        break
                if not found:
                    ix_to_ix[start_index] = ent_ix
                    ix_to_type[start_index] = entity_type
                    ix_to_length[start_index] = entity['length']

        for entity_type, list_to_remove in to_remove.items():
            for ix in reversed(sorted(list_to_remove)):
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
                if entity in entities[entity_type]:
                    entities[entity_type].remove(entity)
                if entity['text'] in geo_by_type[self.geo_transformations[entity['type']]]:
                    continue
                else:
                    geo_ents.append((entity, entity_type))
                    geo_by_type[self.geo_transformations[entity['type']]].append(entity['text'])
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

        # entities = q['prediction']['entities']['$instance']
        # pprint(entities)

        entities = self.entities_normalized(q)
        entities = self.clean_overlapping_entities(entities)

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
            entity_list, query = intent_decoder.decode(
                this_intent, flat_entity_list, prev_q=prev_q)
            query = self.process_entity_list(entity_list, query, lst)
            queries.append(query)

        # if len(geo_ents) > 1:
        #     for geo_ent in geo_ents:
        #         supp_entity_list = [ent for ent in entity_list_ if ent['type'] != '_ConditionSeparator'] + [geo_ent]
        #         supp_entity_list, supp_query = intent_decoder.decode(
        #             this_intent, supp_entity_list, prev_q=prev_q)
        #         supp_query = self.process_entity_list(supp_entity_list, supp_query)
        #         supplementary_queries.append(supp_query)

        # else:
        #     if this_intent == 'cross-by-field-name':
        #         flat_entity_list = [entity for entity_type in entities for entity in entities[entity_type]]
        #         entity_list, query = intent_decoder.decode(
        #             this_intent, flat_entity_list, prev_q=prev_q)
        #
        #         field_names = findFieldNames(entity_list)
        #         if len(field_names) < 2:
        #             print('FATAL ERROR: Not enough fields for Cross')
        #             return
        #
        #         field_name_1 = field_names[0]
        #         field_name_2 = field_names[1]
        #
        #         values_1 = self.get_field_values(field_name_1, query.tables[0], query_processor)
        #         values_1 = [value[field_name_1] for value in values_1['Output']]
        #
        #         values_2 = self.get_field_values(field_name_2, query.tables[0], query_processor)
        #         values_2 = [value[field_name_2] for value in values_2['Output']]
        #
        #         for value_1 in values_1:
        #             entity_list_ = copy.copy(entity_list)
        #             query_ = copy.deepcopy(query)
        #             entity_list_.append({'entity': value_1, 'type': field_name_1})
        #             query_.conditions.append(['eq', '{}.{}'.format(query_.tables[0], field_name_1), value_1])
        #             for value_2 in values_2:
        #                 entity_list_.append({'entity': value_2, 'type': field_name_2})
        #             query_.is_compare = True
        #             query_ = self.process_entity_list(entity_list_, query_)
        #             queries.append(query_)
        #
        #         union = True

        # else:
        #     flat_entity_list = [entity for entity_type in lst for entity in lst[entity_type]]
        #     entity_list, query = intent_decoder.decode(
        #         this_intent, flat_entity_list, prev_q=prev_q, is_a_prev_query=is_a_prev_query)
        #     query = self.process_entity_list(entity_list, query)
        # grouping_fields = [field[0].lower() for field in query.groups]
        # condition_fields = [cond[1].lower() for cond in query.conditions]
        # for entity in entity_list:
        #     if entity['type'] == '_FieldName' and entity['entity'].lower() not in grouping_fields and not any(
        #             [cond_field for cond_field in condition_fields if entity['entity'].lower() in cond_field[1]]):
        #         query.groups.append((entity['entity'], entity['entity']))
        # queries.append(query)

        return queries, union, supplementary_queries


class QueryProcessor:
    def __init__(self, db_config):
        self.db_config = db_config

    def generate_and_run_query(self, qb):

        sql = self.generate_query(qb)
        output = self.run_query(sql, qb.getAllSelects())

        return output, sql

    def generate_query(self, qb):
        qbr = QueryBlockRenderer()
        sql = qbr.render(qb)
        return sql

    def run_query(self, sql, headers):
        msr = MSSQLReader(self.db_config)
        msr.connect(msr.server)
        result = msr.query(sql)
        rows = []
        for ix, row in enumerate(result):
            row_dictionary = {}
            col_index = 0
            for col in headers:
                row_dictionary[col[1]] = row[col_index]
                col_index = col_index + 1
            rows.append(row_dictionary)
        output = {'Output': rows}
        return output

    def test(self):
        # global mssql_server

        print("Testing simple_count:")
        query_test_blocks = QueryTestBlocks()
        qb = query_test_blocks.simple_count()
        print(self.generate_and_run_query(qb))
        print("Testing simple_count_with_date_filter:")
        qb = query_test_blocks.simple_count_with_date_filter()
        print(self.generate_and_run_query(qb))

        print("Testing simple_count and join:")
        qb = query_test_blocks.simple_count_and_join()
        print(self.generate_and_run_query(qb))

        print("Testing simple_count_and_grouped_join:")
        qb = query_test_blocks.simple_count_and_grouped_join()
        print(self.generate_and_run_query(qb))


class AnswerzProcessor():
    def __init__(self, data_map, db_config, luis_config):
        self.intentProcessor = LuisIntentProcessor(data_map)
        self.queryProcessor = QueryProcessor(db_config)
        self.luis_config = luis_config
        self.db_config = db_config
        self.prev_query = None

    def update_prev_query(self, query):
        self.prev_query = query

    def interpret(self, text):
        staging = "true"
        luis_app_id = self.luis_config["luis_app_id"]
        luis_subscription_key = self.luis_config["luis_subscription_key"]

        url = "https://westus.api.cognitive.microsoft.com/luis/prediction/v3.0/apps/{}/slots/staging/predict?subscription-key={}&verbose=true&show-all-intents=true&log=true&query={}".format(
            luis_app_id, luis_subscription_key, text)
        r = requests.get(url=url)
        data = r.json()
        pprint(data)
        return data

    def generate_text_query(self, qb, row):
        # conditions = [cond[-1] + ' ' + cond[1].split('.')[-1] for cond in qb.conditions]
        conditions = []
        for cond in qb.conditions:
            cond_value = cond[-1]
            field_name = cond[1].split('.')[-1]
            if '%' in cond_value:
                if cond[1] in row:
                    cond_value = row[cond[1]]
                else:
                    cond_value = cond_value.replace('%', '')
            if field_name.lower() == 'state':
                cond_value = us.states.lookup(cond_value).name
            conditions.append(str(cond_value) + ' ' + field_name)
        return qb.queryIntent[-1] + ' ' + qb.queryIntent[0] + ' From ' + ' and '.join(conditions)

    def generate_rows_and_cols(self, pq, result):
        if len(result['Output']) == 1:
            row = result['Output'][0]
            cols = [{'field': 'col', 'headerName': '', 'flex': 1},
                    {'field': 'val', 'headerName': '', 'flex': 1}]
            rows = [{'id': ix + 1, 'col': key.replace('_', ' '),  # if '.' not in key else pq.queryIntent[0]
                     'val': val} for
                    ix, (key, val) in enumerate(row.items())]
        else:
            cols = [
                {'field': key,
                 'headerName': key.title().replace('_', ' ') if '.' not in key else pq.queryIntent[0],
                 'flex': 1}
                for key
                in
                list(result['Output'][0].keys())] if \
                result['Output'] else []
            rows = [{'id': ix + 1, **row} for ix, row in enumerate(result['Output'])]
        return rows, cols

    def run_query(self, text, prev_query=None, return_qs=False):
        q = self.interpret(text)
        if prev_query:
            prev_query, union, supp_queries = self.intentProcessor.prepare_query(self.interpret(prev_query), None,
                                                                                 self.queryProcessor,
                                                                                 is_a_prev_query=True)
            prev_query = prev_query[0]
        pqs, union, supp_queries = self.intentProcessor.prepare_query(q, prev_query, self.queryProcessor)
        if pqs:
            self.update_prev_query(pqs[0])  # TODO: fix bug here
        results = []
        qbr = QueryBlockRenderer()
        supp_tables = []
        supp_results = []
        for supp_query in supp_queries:
            supp_result, supp_sql = self.queryProcessor.generate_and_run_query(supp_query)
            supp_rows, supp_cols = self.generate_rows_and_cols(supp_query, supp_result)
            supp_tables.append({'rows': supp_rows, 'cols': supp_cols})
            supp_results.append(supp_result)
        if union and len(pqs[0].groups) < 2:
            sqls = []
            for pq in pqs:
                sql = self.queryProcessor.generate_query(pq)
                sqls.append(sql)
            union_sql = ' union '.join(sqls)
            result = self.queryProcessor.run_query(union_sql, pqs[0].getAllSelects())
            results.append((result, sql))
        else:
            for pq in pqs:
                result, sql = self.queryProcessor.generate_and_run_query(pq)

                cols = [{'field': key, 'headerName': key.title(), 'flex': 1} for key
                        in
                        list(result['Output'][0].keys())] if \
                    result['Output'] else []

                # col = qbr.renderConditions(pq) or 'Calls'
                conds = qbr.renderConditionsInQuery(pq)
                if conds and len(conds) <= 128:
                    col = conds
                else:
                    col = 'Calls'
                total = sum([row[col] for row in result['Output']])
                rows, cols = self.generate_rows_and_cols(pq, result)

                totals_table = self.queryProcessor.generate_and_run_query(pq.totals) if pq.totals else None

                totals_table_cols = [
                    {'field': key, 'headerName': '', 'flex': 1} for key in
                    list(totals_table[0]['Output'][0].keys())] if totals_table and len(
                    totals_table[0]['Output']) > 1 else []

                totals_table_rows = [{'id': ix + 1, **row} for ix, row in enumerate(
                    totals_table[0]['Output'])] if totals_table and len(
                    totals_table[0]['Output']) > 1 else []

                distinct_values_table = self.queryProcessor.generate_and_run_query(pq.distinct_values_query) \
                    if pq.distinct_values_query else None

                distinct_values_table_cols = [
                    {'field': key, 'headerName': '', 'flex': 1} for key in
                    list(distinct_values_table[0]['Output'][0].keys())] if distinct_values_table and len(
                    distinct_values_table[0]['Output']) > 1 else []

                distinct_values_table_rows = [{'id': ix + 1, 'value': self.generate_text_query(pq, row), **row} for
                                              ix, row
                                              in
                                              enumerate(
                                                  distinct_values_table[0]['Output'])] if distinct_values_table and len(
                    distinct_values_table[0]['Output']) > 1 else []

                results.append({'result': result,
                                'sql': sql,
                                'follow_up': True if prev_query else False,
                                'main_table': {'rows': rows, 'cols': cols},
                                'totals': totals_table,
                                'totals_table': {'cols': totals_table_cols,
                                                 'rows': totals_table_rows},
                                'distinct_values': distinct_values_table,
                                'distinct_values_table': {'cols': distinct_values_table_cols,
                                                          'rows': distinct_values_table_rows},
                                'total': total,
                                'supp_tables': supp_tables,
                                'supp_results': supp_results
                                })
        if return_qs:
            return results, pqs, supp_queries
        return results

    def run_sql_query(self, sql, headers):
        result = self.queryProcessor.run_query(sql, headers)
        return result


class CallsColumnTester:
    def __init__(self, col_name, config):
        self.col_name = col_name
        msr = MSSQLReader(config['DB'])
        msr.connect(msr.server)
        results = msr.query(
            'SELECT DISTINCT {} FROM dbo.CallLog3'.format(col_name))
        self.values = [result[0] for result in results]

        self.ap = AnswerzProcessor(
            config['DATAMAP'], config['DB'], config['LUIS'])

    def get_number_of_values(self):
        return len(self.values)

    def run_test(self, sample_size):
        with open('{}_test_results.csv'.format(self.col_name), 'w', newline='') as w:
            writer = csv.writer(w)
            writer.writerow(['Value', 'Query', 'SQL', 'Result'])
            for i in range(sample_size):
                value = self.values[i]
                query = 'How many calls from {}'.format(value)
                _, sql = self.ap.run_query(query)
                sql = ' '.join(sql.split())
                result = 'Pass' if "SELECT Count(distinct CallReportNum) AS Count_CallReportNum FROM CallLog3 WHERE CallLog3.{} = '{}'".format(
                    self.col_name, value).lower() == sql.lower() else 'Fail'
                writer.writerow([value, query, sql, result])


class QueryTester:
    def __init__(self, source, ap):
        self.test_source = source
        self.processor = ap

    def run_tests(self):
        df = pd.read_csv(self.test_source.strip('.csv') + '_w_results.csv')
        df_out = pd.DataFrame(columns=df.columns)
        for ix, row in df.iterrows():
            query = row['query']
            results = self.processor.run_query(query)
            expected_results = row['result']
            if json.dumps(results) == expected_results:
                row['test_result'] = 'PASSED'
            else:
                row['test_result'] = 'FAILED'
            print(row['test_results'] + ' -- ' + row['query'])
            df_out = df_out.append(row)
        df_out.to_csv(self.test_source.strip('.csv') + '_test_results.csv')

    def generate_results(self):
        df = pd.read_csv(self.test_source)
        df_out = pd.DataFrame(columns=df.columns)
        for ix, row in df.iterrows():
            query = row['query']
            results = self.processor.run_query(query)
            row['result'] = json.dumps(results)
            df_out = df_out.append(row)
        df_out.to_csv(self.test_source.strip('.csv') + '_w_results.csv')


if __name__ == '__main__':
    with open('config.json', 'r') as r:
        config = json.loads(r.read())
    ap = AnswerzProcessor(
        config['DATAMAP'], config['DB'], config['LUIS'])
    query_tester = QueryTester('tests.csv', ap)
    # query_tester.generate_results()
    query_tester.run_tests()
