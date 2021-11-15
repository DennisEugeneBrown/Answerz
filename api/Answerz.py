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
        for res in results:
            result = res['result']
            sql = res['sql']
            distinct_values = res['distinct_values']
            if sql.lower() in sql_lower:
                continue
            sql_lower.append(sql.lower())
            out.append(result['Output'])
            print(result['Output'])

        final_ret = {"status": "Success", "message": out, "queries": sql_lower,
                     "other_result": distinct_values[0]['Output'] if distinct_values else ''}
        return final_ret
