from answerz.controller.MSSQLReader import MSSQLReader
from answerz.process.AnswerzProcessor import AnswerzProcessor


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
