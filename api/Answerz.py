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
        args = parser.parse_args()

        text = args['text']
        print(text)
        results = ap.run_query(text)
        sql_lower = []
        out = []
        for result, sql in results:
            if sql.lower() in sql_lower:
                continue
            sql_lower.append(sql.lower())
            out.append(result['Output'])
            print(result['Output'])

        final_ret = {"status": "Success", "message": out, "queries": sql_lower}
        return final_ret
