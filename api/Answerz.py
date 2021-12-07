import app
from answerz_server import AnswerzProcessor
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
        parser = reqparse.RequestParser()
        parser.add_argument('text', type=str)
        parser.add_argument('prev_query', type=str)
        args = parser.parse_args()

        text = args['text']
        prev_query = args['prev_query']
        print(text)
        results, qs = ap.run_query(text, prev_query=prev_query, return_qs=True)
        sql_lower = []
        out = []
        distinct_values = distinct_values_table = None
        totals = totals_table = None
        follow_up = False
        tables = []
        other_results_table = []
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
            sql_lower.append(sql.lower())
            out.append(result['Output'])
            print(result['Output'])
            tables.append({'rows': res['main_table']['rows'], 'cols': res['main_table']['cols']})
        if len(tables) > 1:
            distinct_values = [{'name': list(val[0].keys())[0], 'count': val[0][list(val[0].keys())[0]]} for val in out]
            distinct_values_table_cols = [
                {'field': key, 'headerName': '', 'flex': 1} for key in
                list(distinct_values[0].keys())] if len(distinct_values) > 1 else []
            distinct_values_table_rows = [
                {'id': ix + 1, 'value': qs[ix].queryIntent[-1] + ' ' + qs[ix].queryIntent[0] + ' From ' + ' and '.join(
                    [cond[-1] + ' ' + cond[1].split('.')[-1] for cond in qs[ix].conditions]),
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
                     "message": out,
                     "tables": tables,
                     "queries": sql_lower,
                     "totals": totals[0]['Output'] if totals and len(sql_lower) == 1 else '',
                     "totals_table": totals_table,
                     "distinct_values": distinct_values,
                     "distinct_values_table": distinct_values_table,
                     'follow_up': follow_up}
        return final_ret
