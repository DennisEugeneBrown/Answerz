import io
import os
import re
import sys
import json
import types
import pyodbc
import pprint
import requests
import numpy as np
import pandas as pd

from pprint import pprint

from datetime import datetime
from termcolor import colored


class MSSQLReader:
    def __init__(self, db_config):
        self.connection = None
        self.server = db_config

    def connect(self, server):
        connection_string = 'Driver={};Server={};Database={};Uid={};Pwd={};Port={};'.format(
            self.server["driver"], self.server["host"], self.server["database"], server["user"], server["password"], server["port"])
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
        if not _element in self.DATA_MAP:
            print("FATAL ERROR. Missing mapping for _DataElement = ", _element)
            return
        else:
            mapped_element = self.DATA_MAP[_element]

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
            print("FATAL ERROR. Missing mapping for _DataElement = ", _element)
            return
        else:
            mapped_element = self.DATA_MAP[_element]

        mapped_aggregation = None
        for agg in mapped_element["Aggregations"]:
            if (agg["name"] == _aggregation):
                mapped_aggregation = agg
                break

        if not mapped_aggregation:
            for agg in mapped_element["Aggregations"]:
                if not "default" in agg:
                    continue

                if (agg["default"] == True):
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
        self.groups = []
        self.sorts = []

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
        self.joins.extend(qb_other.joins)
        self.conditions.extend(qb_other.conditions)
        self.groups.extend(qb_other.groups)
        self.sorts.extend(qb_other.sorts)
        return True


# this is a naive renderer with no schema validation
class QueryBlockRenderer:
    def render(self, qb):
        sql = ""
        sql = sql + "\nSELECT\n\t" + self.renderSelect(qb)
        sql = sql + "\nFROM\n\t" + self.renderFrom(qb)

        cond_sql = self.renderConditions(qb)
        if (cond_sql):
            sql = sql + "\nWHERE " + cond_sql

        group_sql = self.renderGroups(qb)
        if (group_sql):
            sql = sql + "\nGROUP BY " + group_sql

        return sql

    def renderSelect(self, qb):
        sep = ""
        sql = ""

        # Handle the group selects
        for term in qb.getAllSelects():
            sql = sql + sep + term[0] + " AS " + term[1]
            sep = ", "

        return sql

    def renderFrom(self, qb):
        sql = qb.table
        if (len(qb.joins)):
            for join in qb.joins:
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
        qb.joins.append(
            ["CallLogNeeds", "CallLog.CallReportNum=CallLogNeeds.CallLogId"])
        return qb

    # This is defined as a class level function not an instance level function. its a way to manage globals
    def simple_count_and_grouped_join(self):
        qb = QueryBlock()
        qb.addTable("CallLog")
        #qb.selects.append(("Count(distinct CallReportNum)", "Measure"))
        qb.addTable("CallLog")
        qb.addTable("CallLogNeeds",
                    "CallLog.CallReportNum=CallLogNeeds.CallLogId")
        #qb.joins.append(["CallLogNeeds", "CallLog.CallReportNum=CallLogNeeds.CallLogId"])
        qb.groups.append(["CallLogNeeds.Need", "Need"])
        return qb

    def chained_select_and_group(self):
        qb1 = QueryBlock()
        qb1.table = "CallLog"
        qb1.selects.append(("Count(distinct CallReportNum)", "Measure"))

        qb2 = QueryBlock()
        qb2.table = "CallLog"
        qb2.joins.append(
            ["CallLogNeeds", "CallLog.CallReportNum=CallLogNeeds.CallLogId"])
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
        for e in entities:
            if (e["type"] == type_name):
                return e["resolution"]["values"][0]
        return None

    def findFieldNames(self, entities):
        fieldNames = []
        for e in entities:
            if (e["type"] == '_FieldName'):
                fieldNames.append(e["resolution"]["values"][0])
        return fieldNames

    # We pass the entire list of entities to the decoder although we expect most to be ignored here

    def decode(self, intent_name, entities, prev_q=None):
        # global DATA_MAP

        _element = self.findEntityByType(entities, "_DataElement")
        _aggregation = self.findEntityByType(entities, "_Aggregations")
        _logicalLabel = self.findEntityByType(entities, "_LogicalLabel")
        _groupAction = self.findEntityByType(entities, "_GroupAction")

        _fieldNames = self.findFieldNames(entities)
        # print('field names:', _fieldNames)

        data_map_repo = DataMapRepo(self.data_map)
        _, mapped_aggregation = data_map_repo.findMapping(
            _element, _aggregation)

        qb = QueryBlock((_element, _aggregation))
        if _groupAction:
            if _fieldNames:
                _, mapped_grouping = data_map_repo.findGrouping(
                    _element, _fieldNames[-1])
                mapped_groupings = [mapped_grouping]
            else:
                # mapped_groupings = data_map_repo.getAllGroupings(_element)
                mapped_groupings = []
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
                            qb.selects.append(["Count(distinct {})".format(
                                col["field"]), "Count_" + col["field"]])
                        else:
                            qb.selects.append(
                                ["Count({})".format(col["field"]), "Count_" + col["field"]])
                    else:
                        qb.selects.append(["Count()", "Count"])

        if _groupAction:
            # print('GROUPS:', qb.groups)
            for mapped_grouping in mapped_groupings:
                if mapped_grouping['joins']:
                    qb.joins.extend(mapped_grouping['joins'])
                if 'display_name' in mapped_grouping:
                    qb.groups.append(
                        (mapped_grouping['field'], mapped_grouping['display_name']))
                else:
                    qb.groups.append(
                        (mapped_grouping['field'], mapped_grouping['name']))

        # qb.addTable("CallLog")

        return qb


class AggregationByLogicalYesDecoder:
    def __init__(self, data_map):
        self.data_map = data_map

    def findEntityByType(self, entities, type_name):
        for e in entities:
            if (e["type"] == type_name):
                return e["resolution"]["values"][0]
        return None

    # We pass the entire list of entities to the decoder although we expect most to be ignored here
    def decode(self, intent_name, entities, prev_q=None):
        # global DATA_MAP

        _element = self.findEntityByType(entities, "_DataElement")
        _aggregation = self.findEntityByType(entities, "_Aggregations")
        _logicalLabel = self.findEntityByType(entities, "_LogicalLabel")
        _groupAction = self.findEntityByType(entities, "_GroupAction")
        _fieldName = self.findEntityByType(entities, "_FieldName")

        data_map_repo = DataMapRepo(self.data_map)
        _, mapped_aggregation = data_map_repo.findMapping(
            _element, _aggregation)

        if _groupAction:
            _, mapped_grouping = data_map_repo.findGrouping(
                _element, _fieldName)

        # print(mapped_grouping)

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
                qb.joins.extend(mapped_grouping['joins'])
            qb.groups.append(
                (mapped_grouping['field'], mapped_grouping['name']))

        # qb.addTable("CallLog")

        return qb


class BreakdownByIntentDecoder:
    def __init__(self, data_map):
        self.data_map = data_map

    def findEntityByType(self, entities, type_name):
        for e in entities:
            if (e["type"] == type_name):
                return e["resolution"]["values"][0]
        return None

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

        _groupAction = self.findEntityByType(entities, "_GroupAction")
        _fieldName = self.findEntityByType(entities, "_FieldName")

        data_map_repo = DataMapRepo(self.data_map)
        _, mapped_aggregation = data_map_repo.findMapping(
            _element, _aggregation)

        # if _groupAction:
        _, mapped_grouping = data_map_repo.findGrouping(
            _element, _fieldName)
        if mapped_grouping['joins']:
            qb.joins.extend(mapped_grouping['joins'])
        qb.groups.append(
            (mapped_grouping['field'], mapped_grouping['name']))

        return qb


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
    def decode(self, entity, query_block):
        # print(entity)
        if 'resolution' in entity:
            if 'values' in entity['resolution']:
                values = entity["resolution"]["values"]
            else:
                values = [entity['resolution']['value']]
        else:
            values = [entity['entity']]

        if (len(values) == 1):

            #aggg:  {'entity': 'how many', 'type': '_Aggregations', 'startIndex': 0, 'endIndex': 7, 'resolution': {'values': ['Count']}}
            #elem:  {'entity': 'calls', 'type': '_DataElement', 'startIndex': 9, 'endIndex': 13, 'resolution': {'values': ['Calls']}}

            entity_name = entity["type"]
            entity_value = values[0]
            print(entity_value)

            lu = self.lookupTablesAndField(
                query_block.queryIntent[0], query_block.queryIntent[1], entity_name, self.data_map)

            if lu:
                tables = lu["tables"]
                field_name = lu["field"]
            else:
                tables = []
                field_name = ''

            # if lu['type'] == 'boolean':
            #     if 'default_value' in lu:
            #         entity_value = lu['default_value']
            #     else:
            #         entity_value = 'YES'

            exact_match = True
            if lu and 'exact_match' in lu and lu['exact_match'] == False:
                exact_match = False

            qb = QueryBlock(query_block.queryIntent)

            for table in tables:
                if (type(table) == str):
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
                                ["eq", field_name, entity_value])
                        else:
                            qb.conditions.append(
                                ["eq", tables[0] + "." + field_name, entity_value])
                    else:
                        if ("." in field_name):  # already scoped
                            qb.conditions.append(
                                ["lk", field_name, '%'+entity_value+'%'])
                        else:
                            qb.conditions.append(
                                ["lk", tables[0] + "." + field_name, '%'+entity_value+'%'])

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
    def decode(self, entity, query_block):
        # print(entity)
        if 'resolution' in entity:
            values = entity["resolution"]["values"]
        else:
            values = [entity]

        if (len(values) == 1):

            #aggg:  {'entity': 'how many', 'type': '_Aggregations', 'startIndex': 0, 'endIndex': 7, 'resolution': {'values': ['Count']}}
            #elem:  {'entity': 'calls', 'type': '_DataElement', 'startIndex': 9, 'endIndex': 13, 'resolution': {'values': ['Calls']}}

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
                            ["lk", field_name, '%'+entity_value+'%'])
                    else:
                        qb.conditions.append(
                            ["lk", tables[0] + "." + field_name, '%'+entity_value+'%'])

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
    def decode(self, entity, query_block=None):
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
    def decode(self, entity, query_block=None):
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

        # Build the initial query block
        query = intent_decoder.decode(
            this_intent, q["entities"], prev_q=prev_q)

        entity_list = []
        pprint(q['entities'])
        for e in q["entities"]:
            entity_list.append(e)

        # pprint(entity_list)

        county_exists = False
        geography_exists = False
        county = None
        geography = None

        for entity in entity_list:
            if entity['type'] == 'County':
                county_exists = True
                county = entity
                break

        for entity in entity_list:
            if 'geography' in entity['type']:
                geography_exists = True
                geography = entity
                break

        if county_exists and geography_exists and\
                (county['resolution']['values'][0].lower() == geography['entity'].lower() or
                 county['resolution']['values'][0].lower() in geography['entity'].lower() or
                 geography['entity'].lower() in county['resolution']['values'][0].lower()): # Differentiate between county and state/city
            county_keyword = False
            for entity in entity_list:
                if entity['entity'].lower() == 'county':
                    county_keyword = True
                    break

            if county_keyword:
                entity_list.remove(geography)
            else:
                entity_list.remove(county)

        # pprint(entity_list)

        for e in entity_list:
            decoder = self.get_entity_decoder(e)
            if (decoder):
                qb = decoder.decode(e, query)

                query.merge(qb)

            elif (not e["type"].startswith("_")):
                print("No decoder for Entity", e["type"])

        return query


class QueryProcessor:
    def __init__(self, db_config):
        self.db_config = db_config

    def generate_and_run_query(self, qb):

        qbr = QueryBlockRenderer()
        sql = qbr.render(qb)

        msr = MSSQLReader(self.db_config)
        msr.connect(msr.server)
        result = msr.query(sql)
        rows = []
        for ix, row in enumerate(result):
            row_dictionary = {}
            col_index = 0
            # print(qb.getAllSelects())
            for col in qb.getAllSelects():

                row_dictionary[col[1]] = row[col_index]
                col_index = col_index + 1
            rows.append(row_dictionary)
        output = {'Output': rows}
        return output, sql

    def generate_query(self, qb):
        qbr = QueryBlockRenderer()
        sql = qbr.render(qb)
        return sql

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

        return data

    def run_query(self, text):
        q = self.interpret(text)
        # pprint(q)
        pq = self.intentProcessor.prepare_query(q, self.prev_query)
        self.update_prev_query(pq)
        result, sql = self.queryProcessor.generate_and_run_query(pq)
        return result, sql


if __name__ == '__main__':
    with open('config.json', 'r') as r:
        config = json.loads(r.read())
    ap = AnswerzProcessor(
        config['DATAMAP'], config['DB'], config['LUIS'])
    result, sql = ap.run_query(
        "how many referrals from mercy house")
    print()
    print(result)
    # print('----------------')
    # result, sql = ap.run_query(
    #     "break it down by gender")
    # pprint(result)
