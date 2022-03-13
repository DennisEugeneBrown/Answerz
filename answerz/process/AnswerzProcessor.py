import us
import requests
from pprint import pprint

from answerz.utils import luis_utils
from answerz.process.QueryProcessor import QueryProcessor
from answerz.process.QueryBlockRenderer import QueryBlockRenderer
from answerz.process.LuisIntentProcessor import LuisIntentProcessor


class AnswerzProcessor:
    def __init__(self, data_map, db_config, luis_config):
        self.luis_config = luis_config
        self.intentProcessor = LuisIntentProcessor(data_map, self.luis_config)
        self.queryProcessor = QueryProcessor(db_config)
        self.db_config = db_config
        self.prev_query = None

    def update_prev_query(self, query):
        self.prev_query = query

    def generate_text_query(self, qb, row):
        # conditions = [cond[-1] + ' ' + cond[1].split('.')[-1] for cond in qb.conditions]
        conditions = []
        for conds in qb.conditions_by_category.values():
            for cond in conds:
                cond_value = cond[-1]
                field_name = cond[1].split('.')[-1]
                if '%' in cond_value:
                    if cond[1] in row:
                        cond_value = row[cond[1]]
                    else:
                        cond_value = cond_value.replace('%', '')
                if field_name.lower() == 'state':
                    cond_value = us.states.lookup(cond_value).name
                conditions.append(str(cond_value) + ' ' + field_name)
        conditions.extend(qb.date_range_conditions.keys())
        return qb.queryIntent[-1] + ' ' + qb.queryIntent[0] + ' From ' + ' and '.join(conditions)

    def generate_rows_and_cols(self, pq, result):
        if len(result['Output']) == 1:
            row = result['Output'][0]
            cols = [{'field': 'col', 'headerName': '', 'flex': 1},
                    {'field': 'val', 'headerName': '', 'flex': 1}]
            rows = [{'id': ix + 1, 'col': key.replace('_', ' '),  # if '.' not in key else pq.queryIntent[0]
                     'val': val} for
                    ix, (key, val) in enumerate(row.items())]
        else:
            cols = [
                {'field': key,
                 'headerName': key.title().replace('_', ' ') if '.' not in key else pq.queryIntent[0],
                 'flex': 1 if key == '' else 0.3, 'resizable': True}
                for key
                in
                list(result['Output'][0].keys())] if \
                result['Output'] else []
            rows = [{'id': ix + 1, **row} for ix, row in enumerate(result['Output'])]
        return rows, cols

    def run_query(self, text, prev_query=None, return_qs=False):
        q = luis_utils.interpret(text, self.luis_config['luis_app_id'], self.luis_config["luis_subscription_key"])
        if prev_query:
            prev_query, union, supp_queries = self.intentProcessor.prepare_query(
                luis_utils.interpret(prev_query, self.luis_config['luis_app_id'],
                                     self.luis_config["luis_subscription_key"]),
                None,
                self.queryProcessor,
                is_a_prev_query=True)
            prev_query = prev_query[0]
        pqs, union, supp_queries = self.intentProcessor.prepare_query(q, prev_query, self.queryProcessor)
        if pqs:
            self.update_prev_query(pqs[0])
        results = []
        qbr = QueryBlockRenderer()
        supp_tables = []
        supp_results = []
        for supp_query in supp_queries:
            supp_result, supp_sql = self.queryProcessor.generate_and_run_query(supp_query)
            supp_rows, supp_cols = self.generate_rows_and_cols(supp_query, supp_result)
            supp_tables.append({'rows': supp_rows, 'cols': supp_cols})
            supp_results.append(supp_result)
        if union and len(pqs[0].groups) < 2:
            sqls = []
            sql = ''
            for pq in pqs:
                sql = self.queryProcessor.generate_query(pq)
                sqls.append(sql)
            union_sql = ' union '.join(sqls)
            result = self.queryProcessor.run_query(union_sql, pqs[0].getAllSelects())
            results.append((result, sql))
        else:
            for pq in pqs:
                result, sql = self.queryProcessor.generate_and_run_query(pq)
                conds = qbr.renderConditionsReadable(pq)
                if conds and len(conds) <= 128:
                    col = conds
                else:
                    col = pq.queryIntent[0]  # eg. Calls
                total = result['OldOutput'][-1][col] if col in result['OldOutput'][-1] else 0
                if not total and result['OldOutput']:
                    total = ', '.join([str(val) for val in result['OldOutput'][0].values() if isinstance(val, int)])

                rows, cols = self.generate_rows_and_cols(pq, result)

                totals_table = self.queryProcessor.generate_and_run_query(pq.totals) if pq.totals else None

                totals_table_cols = [
                    {'field': key, 'headerName': '', 'flex': 1} for key in
                    list(totals_table[0]['Output'][0].keys())] if totals_table and len(
                    totals_table[0]['Output']) > 1 else []

                totals_table_rows = [{'id': ix + 1, **row} for ix, row in enumerate(
                    totals_table[0]['Output'])] if totals_table and len(
                    totals_table[0]['Output']) > 1 else []

                distinct_values_table = self.queryProcessor.generate_and_run_query(pq.distinct_values_query,
                                                                                   distinct_values_query=True) \
                    if pq.distinct_values_query else None

                distinct_values_table_cols = [
                    {'field': key, 'headerName': '', 'flex': 1 if key == 'name' else 0.3} for key in
                    list(distinct_values_table[0]['OldOutput'][0].keys())] if distinct_values_table and len(
                    distinct_values_table[0]['OldOutput']) > 1 else []

                distinct_values_table_rows = [
                    {'id': ix + 1, 'value': self.generate_text_query(pq.distinct_values_query, row), **row} for
                    ix, row
                    in
                    enumerate(
                        distinct_values_table[0][
                            'OldOutput'])] if distinct_values_table and len(
                    distinct_values_table[0]['OldOutput']) > 1 else []

                results.append({'result': result,
                                'sql': sql,
                                'follow_up': True if prev_query else False,
                                'main_table': {'rows': rows, 'cols': cols},
                                'totals': totals_table,
                                'totals_table': {'cols': totals_table_cols,
                                                 'rows': totals_table_rows},
                                'distinct_values': distinct_values_table,
                                'distinct_values_table': {'cols': distinct_values_table_cols,
                                                          'rows': distinct_values_table_rows},
                                'total': total,
                                'supp_tables': supp_tables,
                                'supp_results': supp_results,
                                'query_intent': pq.queryIntent,
                                'groups': pq.groups
                                })
        if return_qs:
            return results, pqs, supp_queries
        return results

    def run_sql_query(self, sql, headers):
        result = self.queryProcessor.run_query(sql, headers)
        return result
