from collections import defaultdict


def process_results(results, qs, qbr, supp_qs):
    out = []
    tables = []
    out_qs = []
    processed_sqls = []
    distinct_values = distinct_values_table = None
    totals = totals_table = None
    follow_up = False
    for res_ix, res in enumerate(results):
        if res['follow_up']:
            follow_up = True
        result = res['result']
        sql = res['sql']
        distinct_values = res['distinct_values']
        distinct_values_table = res['distinct_values_table']
        totals = res['totals']
        totals_table = res['totals_table']
        if sql.lower() in processed_sqls:
            continue
        chart_data = []
        extra_rows_by_group = defaultdict(lambda: defaultdict(lambda: 0))
        for group in qs[res_ix].groups[0:1]:
            if group[1] in qs[res_ix].groups_to_skip:
                continue
            col_1 = group[1]
            conds = qbr.renderConditionsReadable(qs[res_ix])
            if conds and len(conds) <= 128:
                col_2 = conds
            else:
                col_2 = qs[res_ix].queryIntent[0]
            extra_cols = []
            extra_rows = []
            for supp_ix, supp_q in enumerate(supp_qs):
                supp_col = qbr.renderConditionsInQuery(supp_q) or supp_q.queryIntent[0]
                extra_cols.append(supp_col)
                rows = []
                for row in res['supp_results'][supp_ix]['Output']:
                    extra_rows_by_group[row[col_1]][supp_col] = row[supp_col]
                    rows.append(row[supp_col])
                extra_rows.append(rows)
            cols = [col for col in res['result']['OldOutput'][0] if
                    col != group[1] and isinstance(res['result']['OldOutput'][0][col],
                                                   int) and col not in ['Difference', 'Difference %']] if \
                res['result'][
                    'OldOutput'] else [col_2]
            if len(cols) > 1:
                cols.remove(conds) if conds in cols else cols.remove(col_2)

            chart_data.append([col_1] + list(reversed(cols)) + extra_cols)
            for ix, row in enumerate(res['result']['OldOutput'][:-1]):
                if row[col_1] in extra_rows_by_group:  # Fill up chart data for the corresponding group values
                    chart_data.append(
                        [str(row[col_1])] + [row[col] for col in list(reversed(cols))] + [
                            str(extra_rows_by_group[row[col_1]][col]) for col in extra_cols])
                else:  # Fill up chart data for any missing years (groups)
                    chart_data.append(
                        [str(row[col_1])] + [row[col] for col in list(reversed(cols))] + ['' for col in extra_cols])

        out_qs.append(qs[res_ix])
        processed_sqls.append(sql.lower())
        out.append(result['OldOutput'])
        print(result['Output'])
        # chart_data = chart_data[0:1] + sorted(chart_data[1:], key=lambda x: x[1], reverse=True)
        tables.append(
            {'rows': res['main_table']['rows'],
             'cols': res['main_table']['cols'],
             'chart_data': chart_data,
             'total': res['total']})
    return out, tables, out_qs, processed_sqls, distinct_values, distinct_values_table, totals, totals_table, follow_up


def generate_distinct_values_table(tables, out_qs, ap, qs, qbr, out, distinct_values, distinct_values_table):
    if len(tables) > 1:
        distinct_values = [
            {'name': qbr.renderConditionsReadable(out_qs[ix]),
             'count': sum([v[qbr.renderConditionsReadable(out_qs[ix])] for v in val])} for
            ix, val in enumerate(out)]
        distinct_values_table_cols = [
            {'field': key, 'headerName': '', 'flex': 1 if key == 'value' else 0.3} for key in
            ['value', 'count']] if len(distinct_values) > 1 else []
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

    return distinct_values, distinct_values_table


def generate_chart_properties(text, sqls, results):
    # return
    return {'chart_name': text,
            'command': text,
            'sql': '\n'.join([sql.upper() for sql in sqls]),
            'image': None,
            'rows': ', '.join(results[0]['query_intent']),
            'columns': ', '.join(results[0]['result']['Output'][0].keys()),
            'data': ', '.join(results[0]['query_intent']),
            'active_filters': '',
            'chart_title': '',
            'x_axis_label': '',
            'y_axis_label': '',
            'weight': '',
            'show_percent_in_labels': '',
            'show_values_in_labels': '',
            'filters': ''}
