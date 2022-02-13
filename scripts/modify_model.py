import time
import json
import pyodbc
from pprint import pprint
from functools import reduce
from msrest.authentication import CognitiveServicesCredentials
from azure.cognitiveservices.language.luis.runtime import LUISRuntimeClient
from azure.cognitiveservices.language.luis.authoring import LUISAuthoringClient
from azure.cognitiveservices.language.luis.authoring.models import WordListObject


conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=answerz.database.windows.net,1433;'
                      'Database=answerzdb;'
                      'UID=root1234;'
                      'PWD=Bffspoew1234')
cursor = conn.cursor()

authoringKey = 'f5a91b110a204b608b4562286b009ff7'
authoringResourceName = "answerz-authoring"
predictionResourceName = "answerz"

authoringEndpoint = f'https://{authoringResourceName}.cognitiveservices.azure.com/'
predictionEndpoint = f'https://{predictionResourceName}.cognitiveservices.azure.com/'

appName = "Answerz_CallReports"
app_id = 'ad7b56ef-a4c5-4538-a19a-6add68451f43'
versionId = "0.1"
intentName = "agg-elements-by-description"

client = LUISAuthoringClient(
    authoringEndpoint, CognitiveServicesCredentials(authoringKey))

# Adding a new List Entity for Need

rows = cursor.execute(
    "SELECT DISTINCT AIRSNeedCategory FROM CallLogNeeds3 where AIRSNeedCategory is not null and AIRSNeedCategory not like '' AND AIRSNeedCategory NOT LIKE 'unknown'")

items = []
for row in rows:
    items.append(WordListObject(
        canonical_form=row[0], list=[]))

print(len(items))

client.model.add_closed_list(app_id, versionId, sub_lists=items, name='AIRSNeedCategory')


###########################################################################################
# Adding a new List Entity for City

# rows = cursor.execute(
#     "SELECT DISTINCT city FROM calllog3 where city is not null and city not like '' AND city NOT LIKE 'unknown'")

# items = []
# for row in rows:
#     items.append(WordListObject(
#         canonical_form=row[0], list=[row[0].strip() + ' City']))

# print(len(items))

# client.model.add_closed_list(app_id, versionId, sub_lists=items, name='Cities')


###########################################################################################
# Modifying the County Entity List to resolve conflicts between city and county

# # Grab the conflict counties
# cursor.execute(
#     "select distinct county from dbo.CallLog3 where city = county and city != '' and city != 'Unknown'")
# counties = [county[0] for county in cursor]

# # Find the IDs for their sublists under the County list entity and update them
# county_entity_id = '0cd83d65-8023-4bec-bf40-c840139ae175'
# county_entity = client.model.get_closed_list(
#     app_id, versionId, county_entity_id)

# for sublist in county_entity.sub_lists:
#     if sublist.canonical_form in counties:
#         print(sublist.canonical_form)
#         client.model.update_sub_list(app_id, versionId, county_entity_id,
#                                      sublist.id, canonical_form=sublist.canonical_form+' County', list=[])


###########################################################################################
# Adding a new List Entity for Language

# rows = cursor.execute(
#     "select distinct language from dbo.CallLog3 where language != ''")

# items = []
# for row in rows:
#     print(row[0])
#     items.append(WordListObject(
#         canonical_form=row[0], list=[row[0].strip() + ' Language']))

# print()
# print(len(items))

# client.model.add_closed_list(
#     app_id, versionId, sub_lists=items, name='Language')

###########################################################################################
# # Adding a new List Entity for BranchOfService

# rows = cursor.execute(
#     "select distinct VetDischargeStatus from dbo.CallLog3 where VetDischargeStatus != '' and VetDischargeStatus != 'Declined to Answer' and VetDischargeStatus != 'Not Asked' and BranchOfService != 'unknown'")

# items = []
# for row in rows:
#     print(row[0])
#     items.append(WordListObject(
#         canonical_form=row[0], list=[row[0].strip() + ' Status']))

# print()
# print(len(items))

# client.model.add_closed_list(
#     app_id, versionId, sub_lists=items, name='VetDischargeStatus')

###########################################################################################
# Modifying the County Entity List to resolve conflicts between city and county

# Grab the conflict counties
# cursor.execute(
#     "select distinct county from dbo.CallLog3 where city = county and city != '' and city != 'Unknown'")
# counties = [county[0] for county in cursor]
#
# # Find the IDs for their sublists under the County list entity and update them
# county_entity_id = '0cd83d65-8023-4bec-bf40-c840139ae175'
# county_entity = client.model.get_closed_list(
#     app_id, versionId, county_entity_id)
#
# for sublist in county_entity.sub_lists:
#     if sublist.canonical_form.replace(' County', '') in counties:
#         print(sublist.canonical_form)
#         client.model.update_sub_list(app_id, versionId, county_entity_id,
#                                      sublist.id, canonical_form=sublist.canonical_form, list=[sublist.canonical_form.replace(' County', '')])

###########################################################################################
