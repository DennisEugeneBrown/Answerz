import json
import pandas as pd


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
