import app
from collections import defaultdict
from answerz_server import AnswerzProcessor, QueryBlockRenderer
from flask_restful import Resource, reqparse


class Answerz(Resource):
    def get(self):
        return {
            'resultStatus': 'SUCCESS',
            'message': "Hello Answerz"
        }

    def post(self):
        # return {"status": "Success", "message": {"Count_CallReportNum": 58113}}

        ap = AnswerzProcessor(
            app.app.config['DATAMAP'], app.app.config['DB'], app.app.config['LUIS'])
        qbr = QueryBlockRenderer()
        parser = reqparse.RequestParser()
        parser.add_argument('text', type=str)
        parser.add_argument('prev_query', type=str)
        args = parser.parse_args()

        text = args['text']
        prev_query = args['prev_query']
        print(text)
        results, qs, supp_qs = ap.run_query(text, prev_query=prev_query, return_qs=True)
        sql_lower = []
        out = []
        distinct_values = distinct_values_table = None
        totals = totals_table = None
        follow_up = False
        tables = []
        other_results_table = []
        out_qs = []
        for res_ix, res in enumerate(results):
            if res['follow_up']:
                follow_up = True
            result = res['result']
            sql = res['sql']
            distinct_values = res['distinct_values']
            distinct_values_table = res['distinct_values_table']
            totals = res['totals']
            totals_table = res['totals_table']
            if sql.lower() in sql_lower:
                continue
            chart_data = []
            extra_rows_by_group = defaultdict(lambda: defaultdict(lambda: 0))
            for group in qs[res_ix].groups:
                if group[1] in qs[res_ix].groups_to_skip:
                    continue
                col_1 = group[1]
                col_2 = qbr.renderConditions(qs[res_ix]) or 'Calls'
                extra_cols = []
                extra_rows = []
                for supp_ix, supp_q in enumerate(supp_qs):
                    supp_col = qbr.renderConditions(supp_q) or 'Calls'
                    extra_cols.append(supp_col)
                    rows = []
                    for row in res['supp_results'][supp_ix]['Output']:
                        extra_rows_by_group[row[col_1]][supp_col] = row[supp_col]
                        rows.append(row[supp_col])
                    extra_rows.append(rows)

                chart_data.append([col_1, col_2] + extra_cols)
                for ix, row in enumerate(res['result']['Output']):
                    if row[col_1] in extra_rows_by_group: # Fill up chart data for the corresponding group values
                        chart_data.append([str(row[col_1]), row[col_2]] + [extra_rows_by_group[row[col_1]][col] for col in extra_cols])
                    else: # Fill up chart data for any missing years (groups)
                        chart_data.append([str(row[col_1]), row[col_2]] + ['' for col in extra_cols])


            out_qs.append(qs[res_ix])
            sql_lower.append(sql.lower())
            out.append(result['Output'])
            print(result['Output'])
            # chart_data = chart_data[0:1] + sorted(chart_data[1:], key=lambda x: x[1], reverse=True)
            tables.append(
                {'rows': res['main_table']['rows'], 'cols': res['main_table']['cols'], 'chart_data': chart_data,
                 'total': res['total']})
        if len(tables) > 1:
            distinct_values = [
                {'name': qbr.renderConditions(out_qs[ix]),
                 'count': sum([v[qbr.renderConditions(out_qs[ix])] for v in val])} for
                ix, val in enumerate(out)]
            distinct_values_table_cols = [
                {'field': key, 'headerName': '', 'flex': 1} for key in
                list(distinct_values[0].keys())] if len(distinct_values) > 1 else []
            distinct_values_table_rows = [
                {'id': ix + 1, 'value': ap.generate_text_query(qs[ix], row),
                 'type': row['name'].split('.')[1].split(' ')[0] if '.' in row['name'] else row['name'],
                 **row} for
                ix, row in enumerate(
                    distinct_values)] if len(distinct_values) > 1 else []
            distinct_values_table = {'cols': distinct_values_table_cols,
                                     'rows': distinct_values_table_rows}
        else:
            distinct_values = distinct_values[0]['Output'] if distinct_values else ''
            distinct_values_table = distinct_values_table
        sql_lower = list(set(sql_lower))
        final_ret = {"status": "Success",
                     "query": text.title(),
                     "message": out,
                     "tables": tables,
                     "queries": sql_lower,
                     "totals": totals[0]['Output'] if totals and len(sql_lower) == 1 else '',
                     "totals_table": totals_table,
                     "distinct_values": distinct_values,
                     "distinct_values_table": distinct_values_table,
                     'follow_up': follow_up,
                     'conditions': ', '.join([qbr.renderConditions(q) for q in out_qs])}
        return final_ret
