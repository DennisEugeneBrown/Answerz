from answerz.process.QueryBlockRenderer import QueryBlockRenderer
from answerz.tests.TestBlock import QueryTestBlocks
from answerz.controller.MSSQLReader import MSSQLReader


class QueryProcessor:
    def __init__(self, db_config):
        self.db_config = db_config

    def generate_and_run_query(self, qb, distinct_values_query=False):

        sql = self.generate_query(qb)
        output = self.run_query(sql, qb.getAllSelects(), distinct_values_query=distinct_values_query, groups=qb.groups)

        return output, sql

    def generate_query(self, qb):
        qbr = QueryBlockRenderer()
        sql = qbr.render(qb)
        return sql

    def format_output(self, rows, headers, headers_no_groups):
        rows[-1][headers[0][-1]] = 'All' if rows[-1][headers[0][-1]] == 'Blank' else rows[-1][headers[0][-1]]
        return {'OldOutput': rows, 'Output': rows}

    def format_output_transposed(self, rows, headers, headers_no_groups):
        transposed_output = []
        rows[-1][headers[0][-1]] = 'All' if rows[-1][headers[0][-1]] == 'Blank' else rows[-1][headers[0][-1]]
        for _, header in headers_no_groups:
            transposed_row = {'': header}
            total = 0
            for row in rows:
                new_header = str(row[headers[0][1]])
                transposed_row[new_header] = row[header]
                # total += float(str(row[header]).replace('%', '')) if row[header] != 'Blank' else 0
            # transposed_row['Total'] = round(total)
            # if rows:
            #     transposed_row['Difference'] = float(str(rows[-1][header]).replace('%', '')) if rows[-1][
            #                                                                                         header] != 'Blank' else 0 - float(
            #         str(rows[0][header]).replace('%', '')) if rows[0][header] != 'Blank' else 0
            transposed_output.append(transposed_row)
        return {'OldOutput': rows, 'Output': transposed_output}

    def run_query(self, sql, headers, distinct_values_query=False, groups=[]):
        msr = MSSQLReader(self.db_config)
        msr.connect(msr.server)
        result = msr.query(sql)
        rows = []
        for ix, row in enumerate(result):
            row_dictionary = {}
            col_index = 0
            for col in headers:
                row_dictionary[col[1]] = row[col_index] if row[col_index] else 'Blank'
                col_index = col_index + 1
            rows.append(row_dictionary)

        if distinct_values_query:
            return {'OldOutput': rows, 'Output': rows}

        headers_no_groups = [header for header in headers if header not in groups]
        return self.format_output(rows, headers, headers_no_groups)

        # if len(rows) > 4:
        #     return self.format_output(rows, headers, headers_no_groups)
        # return self.format_output_transposed(rows, headers, headers_no_groups)

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
