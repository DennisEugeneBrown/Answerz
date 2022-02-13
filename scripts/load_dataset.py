import pyodbc
import numpy as np
import pandas as pd

# Load Needs File
MY_TABLE = 'dbo.CallLogNeeds4'
FIELDS_TO_KEEP = ['CallReportNum',
                  'TaxonomyName',
                  'Level1Name',
                  'Level2Name',
                  'Level3Name',
                  'Level4Name',
                  'Level5Name',
                  'AIRSNeedCategory',
                  'NeedsMet',
                  'ReasonIfUnmetOrPartial']

conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=answerz.database.windows.net,1433;'
                      'Database=answerzdb;'
                      'UID=root1234;'
                      'PWD=Bffspoew1234')
cursor = conn.cursor()
cursor.fast_executemany = True

insert_to_tmp_tbl_stmt = f"INSERT INTO {MY_TABLE} VALUES ({','.join('?' * len(FIELDS_TO_KEEP))})"

df = pd.read_excel('2021 CY NeedsMetUnmet.xlsx')
print(df.shape)
df = df[FIELDS_TO_KEEP]
df = df.drop_duplicates()
df = df.replace(np.nan, '', regex=True)
print(df.shape)

failed_rows = []
values = df.values.tolist()
for i in range(0, len(values), 1000):
    values_to_insert = values[i:i + 1000]
    try:
        cursor.executemany(insert_to_tmp_tbl_stmt, values_to_insert)
    except pyodbc.IntegrityError:
        print('One or more records on the current batch have failed.. loading this batch one by one')
        for row in values_to_insert:
            try:
                cursor.execute(insert_to_tmp_tbl_stmt, *row)
            except pyodbc.IntegrityError:
                print('No match for:', row[0])
                failed_rows.append(row)
        cursor.commit()
    print(f'{len(values_to_insert)} rows inserted to the {MY_TABLE} table')
    cursor.commit()
cursor.close()
conn.close()
