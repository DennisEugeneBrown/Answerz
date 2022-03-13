from flask_restful import Resource, reqparse

from answerz.app import app
from answerz.process.AnswerzProcessor import AnswerzProcessor
from answerz.process.QueryBlockRenderer import QueryBlockRenderer
from answerz.utils.response_utils import process_results, generate_distinct_values_table, generate_chart_properties


class Answerz(Resource):
    def get(self):
        return {
            'resultStatus': 'SUCCESS',
            'message': "Hello Answerz"
        }

    def post(self):
        # return {"status": "Success", "message": {"Count_CallReportNum": 58113}}

        ap = AnswerzProcessor(
            app.config['DATAMAP'], app.config['DB'], app.config['LUIS'])
        qbr = QueryBlockRenderer()
        parser = reqparse.RequestParser()
        parser.add_argument('text', type=str)
        parser.add_argument('prev_query', type=str)
        args = parser.parse_args()

        text = args['text']
        prev_query = args['prev_query']
        print(text)

        results, qs, supp_qs = ap.run_query(text, prev_query=prev_query, return_qs=True)

        out, tables, out_qs, processed_sqls, distinct_values, distinct_values_table, totals, totals_table, follow_up = process_results(
            results, qs, qbr, supp_qs)

        distinct_values, distinct_values_table = generate_distinct_values_table(tables, out_qs, ap, qs, qbr, out,
                                                                                distinct_values, distinct_values_table)

        chart_properties = generate_chart_properties(text, processed_sqls, results)

        processed_sqls = list(set(processed_sqls))
        final_ret = {"status": "Success",
                     "query": text.title(),
                     "message": out,
                     "tables": tables,
                     "queries": processed_sqls,
                     "totals": totals[0]['Output'] if totals and len(processed_sqls) == 1 else '',
                     "totals_table": totals_table,
                     "distinct_values": distinct_values,
                     "distinct_values_table": distinct_values_table,
                     "follow_up": follow_up,
                     "rows": ', '.join([condition[1] for q in qs for condition in q.conditions]),
                     "conditions": ', '.join([qbr.renderConditionsInQuery(q) for q in out_qs]),
                     "chart_properties": chart_properties}
        return final_ret
