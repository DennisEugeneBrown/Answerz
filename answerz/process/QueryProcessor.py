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
        if len(rows) > 4:
            totals = [round(sum([float(str(row[header]).replace('%', '')) for row in rows if row[header] != 'Blank']))
                      for _, header in
                      headers_no_groups]
            differences = [
                (float(str(rows[-1][header]).replace('%', '')) if rows[-1][header] != 'Blank' else 0) - (float(
                    str(rows[0][header]).replace('%', '')) if rows[0][header] != 'Blank' else 0) for _, header in
                headers_no_groups]
            totals_row = {**{header: 'Total' for _, header in headers[:-2]},
                          **{header: totals[ix] for ix, (_, header) in enumerate(headers_no_groups)}}
            differences_row = {**{header: 'Difference' for _, header in headers[:-2]},
                               **{header: differences[ix] if ix < len(headers_no_groups) - 2 else '' for ix, (_, header)
                                  in enumerate(headers_no_groups)}}
            return {'OldOutput': rows, 'Output': rows + [totals_row, differences_row]}
        transposed_output = []
        for _, header in headers_no_groups:
            transposed_row = {'': header}
            total = 0
            for row in rows:
                # for _, new_header in headers[:-2]:
                new_header = str(row[headers[0][1]])
                transposed_row[new_header] = row[header]
                total += float(str(row[header]).replace('%', '')) if row[header] != 'Blank' else 0
            transposed_row['Total'] = round(total)
            if rows:
                transposed_row['Difference'] = float(str(rows[-1][header]).replace('%', '')) if rows[-1][
                                                                                                    header] != 'Blank' else 0 - float(
                    str(rows[0][header]).replace('%', '')) if rows[0][header] != 'Blank' else 0
            transposed_output.append(transposed_row)
        return {'OldOutput': rows, 'Output': transposed_output}

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
