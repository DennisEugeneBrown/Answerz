import io

import us
import os
import re
import csv
import sys
import json
import types
import pyodbc
import pprint
import requests
import traceback
import numpy as np
import pandas as pd
from pprint import pprint
from datetime import datetime
from termcolor import colored
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
        self.table = None
        self.selects = []
        self.joins = []
        self.conditions = []
        self.count_conditions = []
        self.groups = []
        self.sorts = []
        self.comparators = []
        self.is_compare = False
        self.aggregation = None

    def addTable(self, tableName, join=None):
        if (join):
            for join in self.joins:
                if (join[0] == tableName):
                    #
                    # Already have this join. Let's hope it doesnt conflict!
                    #
                    break

            self.joins.append((tableName, join))
        else:
            self.table = tableName

    def addGroup(self, group_field):
        self.groups.append(group_field)

    def getAllSelects(self):
        # the order here is intentionally backwards. we pre-end the select
        # This is a way to ensure the array is COPIED and not REFERENCED. as we will be modifying the array
        allSelects = self.groups[:]
        allSelects.extend(self.selects)
        return allSelects

    def merge(self, qb_other):
        if (not qb_other):
            return False
        if (qb_other.table and qb_other.table != self.table):
            print("Root table mismatch on Query Block merge")
            print(qb_other.table)
            print(self.table)
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
        return True


# this is a naive renderer with no schema validation
class QueryBlockRenderer:
    def render(self, qb):
        sql = ""
        if qb.count_conditions:
            qb.selects = self.processCountConditions(qb, agg=qb.queryIntent[1].upper())
        sql = sql + "\nSELECT\n\t" + self.renderSelect(qb)
        sql = sql + "\nFROM\n\t" + self.renderFrom(qb)

        cond_sql = self.renderConditions(qb)
        if (cond_sql):
            sql = sql + "\nWHERE " + cond_sql

        group_sql = self.renderGroups(qb)
        if (group_sql):
            sql = sql + "\nGROUP BY " + group_sql

        order_sql = self.renderSorts(qb)
        if (order_sql):
            sql = sql + "\nORDER BY " + order_sql

        return sql

    def renderSelect(self, qb):
        sep = ""
        sql = ""

        # Handle the group selects
        for term in qb.getAllSelects():
            sql = sql + sep + term[0] + " AS [" + term[1] + "]"
            sep = ", "

        return sql

    def renderFrom(self, qb):
        sql = qb.table
        if (len(qb.joins)):
            for join in set(qb.joins):
                sql = sql + "\n\tJOIN " + join[0] + " ON " + join[1]
        return sql

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

    def renderConditions(self, qb):
        sep = ""
        sql = ""

        def encodeLHS(lhs):

            return lhs

        def encodeRHS(rhs):
            return "'" + rhs + "'"

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
            return op

        # Handle the group selects
        for term in qb.conditions:
            sql = sql + sep + encodeCondition(term)
            sep = " AND "

        return sql

    def processCountConditions(self, qb, agg='COUNT'):
        selects = []

        def encodeSelect(lhs, rhs, agg, encoded_op):
            if agg.lower() == 'avg':
                field = "CAST(dbo.{}.{} AS {})".format(qb.table, 'CallLength', 'INT')
            else:
                field = 1

            selects.append(["{agg}(IIF({lhs} {encoded_op} '{rhs}', {field}, NULL))".format(lhs=lhs, rhs=rhs, agg=agg,
                                                                                           encoded_op=encoded_op,
                                                                                           field=field),
                            '{agg}_{rhs}'.format(agg=agg, rhs=rhs)])

        def encodeCondition(cond, agg):
            op, lhs, rhs = cond

            if (op == "eq"):
                encodeSelect(lhs, rhs, agg, " = ")
            if (op == "lk"):
                encodeSelect(lhs, rhs, agg, " like ")
            if (op == "lt"):
                encodeSelect(lhs, rhs, agg, " < ")
            if (op == "lte"):
                encodeSelect(lhs, rhs, agg, " <= ")
            if (op == "gt"):
                encodeSelect(lhs, rhs, agg, " > ")
            if (op == "gte"):
                encodeSelect(lhs, rhs, agg, " >= ")

        other_selects = set()
        for term in qb.conditions:
            other_selects.add(tuple(term))
        for term in other_selects:
            selects.append(["'" + term[2] + "'", term[1]])

        # Handle the group selects
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
        qb1.table = "CallLog"
        qb1.selects.append(("Count(distinct CallReportNum)", "Measure"))

        qb2 = QueryBlock()
        qb2.table = "CallLog"
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

    def findEntityByType(self, entities, type_name):
        for ix, e in enumerate(entities):
            if e["type"] == type_name:
                return ix, e["resolution"]["values"][0]
        return -1, None

    def findFieldNames(self, entities):
        fieldNames = []
        for e in entities:
            if (e["type"] == '_FieldName'):
                fieldNames.append(e["resolution"]["values"][0])
        return fieldNames

    # We pass the entire list of entities to the decoder although we expect most to be ignored here

    def decode(self, intent_name, entities, prev_q=None):
        # global DATA_MAP

        _element_ix, _element = self.findEntityByType(entities, "_DataElement")
        _aggregation_ix, _aggregation = self.findEntityByType(entities, "_Aggregations")
        _logicalLabel_ix, _logicalLabel = self.findEntityByType(entities, "LogicalLabel")
        _groupAction_ix, _groupAction = self.findEntityByType(entities, "_GroupAction")
        _comparator_ix, _comparator = self.findEntityByType(entities, "_Comparator")

        _fieldNames = self.findFieldNames(entities)

        data_map_repo = DataMapRepo(self.data_map)
        mapped_element, mapped_aggregation = data_map_repo.findMapping(
            _element, _aggregation)

        update_number = None
        for field_name in _fieldNames:
            for dim in mapped_element["Dimensions"]:
                if dim["name"].lower() == field_name.lower() and dim['type'] == 'int':
                    update_number = dim["name"]

        if update_number:
            for ix, entity in enumerate(entities):
                if entity['type'] == 'builtin.number':
                    entities[ix]['type'] = update_number

        # print('field names:', _fieldNames)

        qb = QueryBlock((_element, _aggregation))

        comparators_mapping = {
            '<': 'lt',
            '>': 'gt',
            '<=': 'lte',
            '>=': 'gte'
        }

        if _comparator:
            qb.comparators.append(comparators_mapping[_comparator])

        mapped_groupings = []
        if _groupAction:
            if _groupAction in ['group by', 'breakdown by']:
                if _fieldNames:
                    _, mapped_grouping = data_map_repo.findGrouping(
                        _element, _fieldNames[-1])
                    mapped_groupings = [mapped_grouping]
                elif _logicalLabel:
                    _, mapped_grouping = data_map_repo.findGrouping(
                        _element, _logicalLabel)
                    mapped_groupings = [mapped_grouping]
                    del entities[_logicalLabel_ix]
                else:
                    # mapped_groupings = data_map_repo.getAllGroupings(_element)
                    pass
            elif _groupAction.lower() == 'compare':
                qb.is_compare = True

            # print(mapped_groupings)

        # print(mapped_grouping)

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
                                qb.table, col["field"]), "Count_" + col["field"]])
                        else:
                            qb.selects.append(
                                ["Count({})".format(col["field"]), "Count_" + col["field"]])
                    else:
                        qb.selects.append(["Count()", "Count"])
                elif (col['agg'] == 'avg'):
                    if "field" in col and col["field"]:
                        if 'cast' in col and col['cast']:
                            field = "CAST({}dbo.{}.{} AS {})".format('distinct ' if col['distinct'] else '', qb.table,
                                                                     col["field"], col['cast'])
                        else:
                            field = "{}dbo.{}.{}".format('distinct ' if col['distinct'] else '', qb.table, col["field"])

                        qb.selects.append(["Avg({})".format(field), "Avg_" + col['field']])

        if _groupAction:
            # print('GROUPS:', qb.groups)
            if _groupAction in ['group by', 'breakdown by']:
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
                            qb.sorts.append(sort_field)
                            if sort_field[0] != mapped_grouping['field']:
                                qb.groups.append((sort_field[0], sort_field[0]))
                    else:
                        qb.sorts.append((mapped_grouping['field'], 'ASC'))
        # qb.addTable("CallLog")

        return entities, qb


class AggregationByLogicalYesDecoder:
    def __init__(self, data_map):
        self.data_map = data_map

    def findEntityByType(self, entities, type_name):
        for ix, e in enumerate(entities):
            if e["type"] == type_name:
                return ix, e["resolution"]["values"][0]
        return -1, None

    # We pass the entire list of entities to the decoder although we expect most to be ignored here
    def decode(self, intent_name, entities, prev_q=None):
        # global DATA_MAP

        _element_ix, _element = self.findEntityByType(entities, "_DataElement")
        _aggregation_ix, _aggregation = self.findEntityByType(entities, "_Aggregations")
        _logicalLabel_ix, _logicalLabel = self.findEntityByType(entities, "LogicalLabel")
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
                                col["field"]), "Count_" + col["field"]])
                        else:
                            qb.selects.append(
                                ["Count({})".format(col["field"]), "Count_" + col["field"]])
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

    def findEntityByType(self, entities, type_name):
        for ix, e in enumerate(entities):
            if e["type"] == type_name:
                return ix, e["resolution"]["values"][0]
        return -1, None

    # We pass the entire list of entities to the decoder although we expect most to be ignored here
    def decode(self, intent_name, entities, prev_q=None):
        # global DATA_MAP

        qb = prev_q
        try:
            _element = qb.queryIntent[0]  # TODO: Add and use a getter
            _aggregation = qb.queryIntent[1]  # TODO: Add and use a getter
        except:
            print('ERROR. CANNOT FIND PREVIOUS QUERY TO BREAKDOWN.')
            return QueryBlock()

        _groupAction_ix, _groupAction = self.findEntityByType(entities, "_GroupAction")
        _fieldName_ix, _fieldName = self.findEntityByType(entities, "_FieldName")
        _logicalLabel_ix, _logicalLabel = self.findEntityByType(entities, "LogicalLabel")

        data_map_repo = DataMapRepo(self.data_map)
        _, mapped_aggregation = data_map_repo.findMapping(
            _element, _aggregation)

        # if _groupAction:
        if _fieldName:
            _, mapped_grouping = data_map_repo.findGrouping(
                _element, _fieldName)
        elif _logicalLabel:
            _, mapped_grouping = data_map_repo.findGrouping(
                _element, _logicalLabel)
            del entities[_logicalLabel_ix]
        else:
            raise Exception('Something went wrong.')

        if mapped_grouping['joins']:
            qb.joins.extend(tuple(mapped_grouping['joins']))
        qb.groups.append(
            (mapped_grouping['field'], mapped_grouping['name']))
        if 'sort_fields' in mapped_grouping:
            for sort_field in mapped_grouping['sort_fields']:
                qb.sorts.append(sort_field)
                if sort_field[0] != mapped_grouping['field']:
                    qb.groups.append((sort_field[0], sort_field[0]))
        else:
            qb.sorts.append((mapped_grouping['field'], 'ASC'))

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

                return {
                    "tables": tables,
                    "field": dim["field"]
                }

        return None


class ColumnEntityDecoder(EntityDecoderBase):
    def __init__(self, data_map):
        self.data_map = data_map
        pass

    def mapValues(self, table, field_name, entity_value):
        return [entity_value]

    # Takes the entity to decode + a potential query_block to augment
    def decode(self, entity, query_block, comparators=None):
        # print(entity)
        if 'resolution' in entity:
            if 'values' in entity['resolution']:
                values = entity["resolution"]["values"]
            else:
                values = [entity['resolution']['value']]
        else:
            values = [entity['entity']]

        if (len(values) == 1):

            # aggg:  {'entity': 'how many', 'type': '_Aggregations', 'startIndex': 0, 'endIndex': 7, 'resolution': {'values': ['Count']}}
            # elem:  {'entity': 'calls', 'type': '_DataElement', 'startIndex': 9, 'endIndex': 13, 'resolution': {'values': ['Calls']}}

            entity_name = entity["type"]
            entity_value = values[0]
            # print(entity_value)

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

            # if lu['type'] == 'boolean':
            #     if 'default_value' in lu:
            #         entity_value = lu['default_value']
            #     else:
            #         entity_value = 'YES'

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
                if (len(db_values) == 1):

                    if exact_match:
                        if ("." in field_name):  # already scoped
                            qb.conditions.append(
                                [(comparators[0] if comparators else "eq"), field_name, entity_value])
                        else:
                            qb.conditions.append(
                                [(comparators[0] if comparators else "eq"), tables[0] + "." + field_name, entity_value])
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


class LogicalLabelEntityDecoder(EntityDecoderBase):
    def __init__(self, data_map):
        self.data_map = data_map
        pass

    def mapValues(self, table, field_name, entity_value):
        return [entity_value]

    # Takes the entity to decode + a potential query_block to augment
    def decode(self, entity, query_block, comparators=None):
        # print(entity)
        if 'resolution' in entity:
            values = entity["resolution"]["values"]
        else:
            values = [entity]

        if (len(values) == 1):

            # aggg:  {'entity': 'how many', 'type': '_Aggregations', 'startIndex': 0, 'endIndex': 7, 'resolution': {'values': ['Count']}}
            # elem:  {'entity': 'calls', 'type': '_DataElement', 'startIndex': 9, 'endIndex': 13, 'resolution': {'values': ['Calls']}}

            entity_name = entity["type"]
            if 'resolution' in entity:
                entity_value = entity["resolution"]["values"][0]
            else:
                entity_value = entity['entity']

            lu = self.lookupTablesAndField(
                query_block.queryIntent[0], query_block.queryIntent[1], entity_name, self.data_map)

            tables = lu["tables"]
            field_name = lu['field'][entity_value]

            if 'default_value' in lu:
                entity_value = lu['default_value']
            else:
                entity_value = 'YES'

            exact_match = True
            if 'exact_match' in lu and lu['exact_match'] == False:
                exact_match = False

            qb = QueryBlock(query_block.queryIntent)

            for table in tables:
                if (type(table) == str):
                    qb.addTable(table)
                else:
                    # note: this is not yet tested and may break
                    qb.addTable(table[0], table[1])

            db_values = self.mapValues(tables[0], field_name, entity_value)
            if (len(db_values) == 1):

                if exact_match:
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
    def decode(self, entity, query_block=None, comparators=None):
        values = entity["resolution"]["values"]

        if (len(values) == 1):

            value = values[0]
            if (value["type"] == "daterange"):

                entity_type = "datetime"
                entity_value = entity["resolution"]["values"][0]

                lu = self.lookupTablesAndFieldByType(
                    query_block.queryIntent[0], query_block.queryIntent[1], entity_type, self.data_map)
                if not lu:
                    entity_type = "date"
                    lu = self.lookupTablesAndFieldByType(
                        query_block.queryIntent[0], query_block.queryIntent[1], entity_type, self.data_map)

                if (lu):
                    # TODO for compatability other than MSSQL need to lookup seperator
                    field_name = lu["tables"][0] + "." + lu["field"]

                    # Field name hard coded for now. This is wrong.
                    qb = QueryBlock()
                    qb.addTable(query_block.table)

                    qb.conditions.append(
                        ["gte", field_name, entity_value["start"]])
                    if "end" in entity_value:
                        qb.conditions.append(
                            ["lt", field_name, entity_value["end"]])
                    else:
                        qb.conditions.append(
                            ["lt", field_name, datetime.now().strftime('%Y-%m-%d')])

                    return qb

        return None


class DateEntityDecoder(EntityDecoderBase):
    def __init__(self, data_map):
        self.data_map = data_map

    # Takes the entity to decode + a potential query_block to augment
    def decode(self, entity, query_block=None, comparators=None):
        values = entity["resolution"]["values"]

        if (len(values) == 1):

            value = values[0]
            if (value["type"] == "date"):

                entity_type = "datetime"
                entity_value = entity["resolution"]["values"][0]

                lu = self.lookupTablesAndFieldByType(
                    query_block.queryIntent[0], query_block.queryIntent[1], entity_type, self.data_map)
                if not lu:
                    entity_type = "date"
                    lu = self.lookupTablesAndFieldByType(
                        query_block.queryIntent[0], query_block.queryIntent[1], entity_type, self.data_map)

                if (lu):
                    # TODO for compatability other than MSSQL need to lookup seperator
                    field_name = lu["tables"][0] + "." + lu["field"]

                    # Field name hard coded for now. This is wrong.
                    qb = QueryBlock()
                    qb.addTable(query_block.table)

                    qb.conditions.append(
                        ["gte", field_name, entity_value["value"]])
                    qb.conditions.append(
                        ["lte", field_name, entity_value["value"] + ' 23:59:59'])

                    return qb

        return None


class LuisIntentProcessor:

    def __init__(self, data_map):
        # Intent Decoders
        self.i_decoders = {}
        self.i_decoders["agg-elements-by-description"] = AggregationByDescriptionIntentDecoder(
            data_map)
        self.i_decoders["agg-elements-by-logical-yes"] = AggregationByLogicalYesDecoder(
            data_map)
        self.i_decoders["breakdown-by"] = BreakdownByIntentDecoder(
            data_map)

        # Entity Decoders
        self.e_decoder_default = ColumnEntityDecoder(data_map)
        self.e_decoders = {}
        self.e_decoders["builtin.datetimeV2.daterange"] = DateRangeEntityDecoder(
            data_map)
        self.e_decoders["builtin.datetimeV2.date"] = DateEntityDecoder(
            data_map)
        self.e_decoders["LogicalLabel"] = LogicalLabelEntityDecoder(
            data_map)

    def get_intent_decoder(self, intent_name):

        if (intent_name in self.i_decoders):
            return self.i_decoders[intent_name]

        return None

    def get_entity_decoder(self, entity):

        t = entity["type"]
        if (t in self.e_decoders):
            return self.e_decoders[t]

        if (t.startswith("_")):
            # this is a system field
            return None

        return self.e_decoder_default

    def prepare_query(self, q, prev_q):
        self.luis = q

        # First setup the context for the intent assigned by LUIS
        this_intent = q["topScoringIntent"]["intent"]
        intent_decoder = self.get_intent_decoder(this_intent)
        if (not intent_decoder):
            print("Unable to continue. Un-recognized intent: ", this_intent)
            return False

        # Note: I am assuming all intents ARE Query requests, but we know for a fact there is AT LEAST a "None" intent as well as some NAVIGATION intents
        # so this is not such a good midterm assumption

        entity_list = []
        pprint(q['entities'])
        for e in q["entities"]:
            entity_list.append(e)

        county_exists = False
        city_exists = False
        geography_exists = False
        county = None
        city = None
        geography = None

        geography_entity_types = [
            'builtin.geographyV2.state', 'County', 'City', 'builtin.geographyV2.city']

        geography_entities_found = [
            el for el in entity_list if el['type'] in geography_entity_types]

        geography_entities_found = []
        for type in geography_entity_types:
            for entity in entity_list:
                if entity['type'] == type:
                    geography_entities_found.append(entity)

        print('There are {} geography entities.'.format(
            len(geography_entities_found)))
        print(geography_entities_found)

        # for entity in entity_list:
        #     if entity['type'] == 'County':
        #         county_exists = True
        #         county = entity
        #         break

        # for entity in entity_list:
        #     if entity['type'] == 'City':
        #         city_exists = True
        #         city = entity
        #         break

        # for entity in entity_list:
        #     if 'geography' in entity['type']:
        #         geography_exists = True
        #         geography = entity
        #         break

        # if county_exists and geography_exists and\
        #         (county['resolution']['values'][0].lower() == geography['entity'].lower() or
        #          county['resolution']['values'][0].lower() in geography['entity'].lower() or
        #          geography['entity'].lower() in county['resolution']['values'][0].lower()):  # Differentiate between county and state/city
        #     county_keyword = False
        #     for entity in entity_list:
        #         if entity['entity'].lower() == 'county':
        #             county_keyword = True
        #             break

        #     if county_keyword:
        #         entity_list.remove(geography)
        #     else:
        #         entity_list.remove(county)

        # pprint(entity_list)

        keep = None
        if 'county' in q['query'].lower():
            keep = ['county']
        elif 'city' in q['query'].lower():
            keep = ['city', 'builtin.geographyV2.city']
        elif 'state' in q['query'].lower():
            keep = ['builtin.geographyV2.state']

        queries = []

        out = []

        ix_to_ix = {}

        to_remove = []

        for ent_ix, entity in enumerate(entity_list):
            start_index = entity['startIndex']
            if start_index in ix_to_ix:
                if entity_list[ix_to_ix[start_index]]['type'].startswith('_'):
                    to_remove.append(ent_ix)
                elif entity['type'].startswith('_'):
                    to_remove.append(ix_to_ix[start_index])
                    ix_to_ix[start_index] = ent_ix
            else:
                ix_to_ix[start_index] = ent_ix

        for ix in reversed(sorted(to_remove)):
            del entity_list[ix]

        # ix_to_element[entity['startIndex']] =

        if len(geography_entities_found) > 1:
            # Build the initial query block
            for entity in geography_entities_found:
                if keep and entity['type'].lower() not in keep:
                    continue

                entity_list, query = intent_decoder.decode(
                    this_intent, q["entities"], prev_q=prev_q)

                # entity_list.remove(entity)
                entity_list_ = [
                    entity_ for entity_ in entity_list if entity_ not in geography_entities_found or entity_ == entity]

                for e in entity_list_:
                    decoder = self.get_entity_decoder(e)
                    if (decoder):
                        print('Decoding {}...'.format(e))
                        if query.comparators:
                            qb = decoder.decode(e, query, comparators=query.comparators)
                        else:
                            qb = decoder.decode(e, query)

                        query.merge(qb)

                    elif (not e["type"].startswith("_")):
                        print("No decoder for Entity", e["type"])

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

                queries.append(query)
        else:

            entity_list, query = intent_decoder.decode(
                this_intent, entity_list, prev_q=prev_q)

            for e in entity_list:
                decoder = self.get_entity_decoder(e)
                if (decoder):
                    print('Decoding {}...'.format(e))
                    if query.comparators:
                        qb = decoder.decode(e, query, comparators=query.comparators)
                    else:
                        qb = decoder.decode(e, query)

                    query.merge(qb)

                elif (not e["type"].startswith("_")):
                    print("No decoder for Entity", e["type"])

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

            queries.append(query)

        return queries


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

        url = "https://westus.api.cognitive.microsoft.com/luis/v2.0/apps/{}?staging={}&verbose=true&timezoneOffset=-360&subscription-key={}".format(
            luis_app_id, staging, luis_subscription_key)
        r = requests.get(url=url + "&q=" + text)
        data = r.json()
        pprint(data)
        return data

    def run_query(self, text):
        q = self.interpret(text)
        # pprint(q)
        pqs = self.intentProcessor.prepare_query(q, self.prev_query)
        self.update_prev_query(pqs[0])  # TODO: fix bug here
        results = []
        for pq in pqs:
            result, sql = self.queryProcessor.generate_and_run_query(pq)
            results.append((result, sql))
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


if __name__ == '__main__':
    with open('config.json', 'r') as r:
        config = json.loads(r.read())
    tester = CallsColumnTester('city', config)
    tester.run_test(30)

    # ap = AnswerzProcessor(
    #     config['DATAMAP'], config['DB'], config['LUIS'])
    # result, sql = ap.run_query(
    #     "how many referrals from mercy house")
    # print()
    # print(result)
    # print('----------------')
    # result, sql = ap.run_query(
    #     "break it down by gender")
    # pprint(result)
