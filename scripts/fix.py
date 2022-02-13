import re
import csv

with open('city_test_results_deprecated.csv', 'r') as r, open('city_test_results.csv', 'w', newline='') as w:
    reader = csv.reader(r)
    writer = csv.writer(w)
    columns = next(reader)
    writer.writerow(['Query', 'SQL', 'Value', 'Result'])
    for row in reader:
        value = row[0]
        query = row[1]
        sql = row[2]
        if re.search(r"SELECT Count\(distinct CallReportNum\) AS Count_CallReportNum FROM CallLog3 WHERE CallLog3\.{} = '.+'".format('city').lower(), sql.lower()):
            # if "SELECT Count(distinct CallReportNum) AS Count_CallReportNum FROM CallLog3 WHERE CallLog3.{} = '{}'".format('city', value).lower() == sql.lower():
            result = 'Pass'
        else:
            result = 'Fail'
        writer.writerow([query, sql, value, result])
