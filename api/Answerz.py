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
        results = ap.run_query(text, prev_query=prev_query)
        sql_lower = []
        out = []
        distinct_values = None
        follow_up = False
        tables = []
        other_results_table = []
        for res in results:
            if res['follow_up']:
                follow_up = True
            result = res['result']
            sql = res['sql']
            distinct_values = res['distinct_values']
            other_results_table = res['distinct_values_table']
            totals_table = res['totals_table']
            if sql.lower() in sql_lower:
                continue
            sql_lower.append(sql.lower())
            out.append(result['Output'])
            print(result['Output'])
            tables.append({'rows': res['main_table']['rows'], 'cols': res['main_table']['cols']})
        sql_lower = list(set(sql_lower))
        final_ret = {"status": "Success",
                     "message": out,
                     "tables": tables,
                     "queries": sql_lower,
                     "other_result": distinct_values[0]['Output'] if distinct_values else totals_table[0][
                         'Output'] if totals_table and len(sql_lower) == 1 else '',
                     "other_result_table": other_results_table,
                     'follow_up': follow_up}
        return final_ret
