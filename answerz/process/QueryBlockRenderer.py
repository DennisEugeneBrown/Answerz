from answerz.model.QueryBlock import QueryBlock


# this is a naive renderer with no schema validation
class QueryBlockRenderer:
    def render(self, qb):
        sql = ""

        if qb.count_conditions:
            qb.selects = self.processCountConditions(qb,
                                                     agg=qb.queryIntent[1].upper() if qb.queryIntent[1] else 'COUNT')

        cond_sql = self.renderConditionsInQuery(qb)
        cond_select = self.renderConditionsReadable(qb)
        if cond_select and len(cond_select) <= 128:
            for ix, select in enumerate(qb.selects):
                if 'Count_' in select[1]:
                    for sort_ix, sort in enumerate(qb.sorts):
                        if select[1] == sort[0]:
                            qb.sorts[sort_ix] = ('[' + cond_select + ']', sort[1])
                    qb.selects[ix][1] = cond_select

            qb.selects[0][1] = cond_select

        if qb.with_query and qb.conditions:
            qb.with_query.selects = [['COUNT(*)', 'Total_Responses'], ["SUM({})".format(
                ' * '.join(["IIF({} != '', 1, 0)".format(cond[1]) for cond in qb.conditions])), 'Valid_Responses']]
        add_with_table = False
        if (not qb.groups and qb.with_query and qb.conditions) or qb.is_total:
            add_with_table = True
            if 'total' not in qb.tables:
                qb.addTable('total')
            if not qb.is_total:
                # qb.groups.append([qb.with_query.selects[0][-1], qb.with_query.selects[0][-1]])
                for select in qb.with_query.selects:
                    qb.selects.append([select[-1], select[-1]])
                    qb.groups.append([select[-1], select[-1]])
                qb.groups_to_skip.append(qb.with_query.selects[0][-1])
                qb.selects.append(
                    ["CAST(CAST(COUNT(*) * 100.0 / {} AS decimal(10, 2)) AS varchar) + '%'".format(
                        qb.with_query.selects[0][-1]),
                        'Percentage'.format(qb.with_query.selects[-1][-1])])

            if not qb.is_total:
                qb.totals = QueryBlock()
                qb.totals.is_total = True
                qb.totals.addTable(qb.tables)
                qb.totals.with_query = QueryBlock()
                qb.totals.with_query.selects = [[qb.selects[0][0], 'Total_Records']]
                qb.totals.with_query.tables = qb.tables[0:1]
                qb.totals.with_query.joins = qb.joins
                qb.totals.groups.append(['Total_Records', 'Total_Records'])
                if qb.conditions:
                    qb.totals.with_query.selects.append(["SUM({})".format(
                        ' * '.join(["IIF({} != '', 1, 0)".format(cond[1]) for cond in qb.conditions])),
                        'Valid_Responses'])
                    qb.totals.with_query.selects.append(["SUM({})".format(
                        ' * '.join(["IIF({} = '', 1, 0)".format(cond[1]) for cond in qb.conditions])),
                        'Blanks_or_Nulls'])
                    qb.totals.groups.append(['Valid_Responses', 'Valid_Responses'])
                    qb.totals.groups.append(['Blanks_or_Nulls', 'Blanks_or_Nulls'])
                qb.totals.unpivot_selects = [['col', 'col'], ['value', 'value'],
                                             [
                                                 "cast(cast(value * 100.0 / [Total_Records] as decimal(10, 2)) as varchar) + '%'",
                                                 'percentage']]
                qb.totals.unpivot_cols = [col[0] for col in qb.totals.groups]

        sql = sql + "\nSELECT\n\t" + self.renderSelect(qb)
        sql = sql + "\nFROM\n\t" + self.renderFrom(qb)

        if cond_sql:
            sql = sql + "\nWHERE " + cond_sql

        group_sql = self.renderGroups(qb)
        if group_sql:
            sql = sql + "\nGROUP BY " + group_sql

        if qb.unions and len(qb.groups) < 2:
            qbr = QueryBlockRenderer()
            qb2 = qbr.render(qb.unions[0])
            sql += ' UNION ' + qb2

        order_sql = self.renderSorts(qb)
        if order_sql:
            sql = sql + "\nORDER BY " + order_sql

        if qb.unpivot_cols:
            sql = self.renderUnpivot(qb, sql)
            print('sql is')

        if add_with_table:
            qbr = QueryBlockRenderer()
            sql = 'WITH total AS({})'.format(qbr.render(qb.with_query)) + sql

        return sql

    def renderUnpivotSelect(self, qb):
        sep = ""
        sql = ""
        for term in qb.unpivot_selects:
            sql = sql + sep + term[0] + " AS [" + term[1] + "]"
            sep = ", "
        return sql

    def renderUnpivot(self, qb, sql):
        sql = 'SELECT ' + self.renderUnpivotSelect(qb) + ' FROM ({})'.format(sql)
        sql = sql + 'AS SourceTable UNPIVOT (value for col in ({})) AS PivotTable, total;'.format(
            ','.join(qb.unpivot_cols))
        return sql

    def renderSelect(self, qb):
        sep = ""
        sql = ""

        # Handle the group selects
        for term in qb.getAllSelects():
            sql = sql + sep + term[0] + " AS [" + term[1] + "]"
            sep = ", "

        return sql

    def renderFrom(self, qb, add_with_table=False):
        sql = ''
        for table in qb.tables:
            sql += table
            # if add_with_table:
            #     sql += ', total'
            if len(qb.joins):
                for join in set([tuple(join) for join in qb.joins]):
                    if table in join[1].split('.'):
                        sql = sql + "\n\tJOIN " + join[0] + " ON " + join[1]
            sql += ', '
        return sql.strip().rstrip(',')

    def renderGroups(self, qb):
        sep = ""
        sql = ""

        # Handle the group selects
        for term in qb.groups:
            sql = sql + sep + term[0]
            sep = ", "

        return sql

    def renderSorts(self, qb):
        sep = ""
        sql = ""

        # Handle the group selects
        for term in qb.sorts:
            if term[1] == 'ASC':
                sql = sql + sep + \
                      "case when ({value} is null or {value} like '') then 1 else 0 end, {value} ASC".format(
                          value=term[0])
            else:
                sql = sql + sep + ' '.join(term)
            sep = ", "

        return sql

    def renderConditionsInQuery(self, qb):
        return self.renderConditions(qb.conditions_by_category, qb.date_range_conditions, cond_sep=qb.cond_sep)

    def renderConditionsReadable(self, qb):
        conditions = qb.conditions_by_category
        date_conditions = qb.date_range_conditions

        out = ""

        def encodeOp(cond):
            op, field, val = cond
            if op == "eq" or op == "lk":
                return ''
            if op == "lt":
                return field.split('.')[-1] + " < "
            if op == "lte":
                return field.split('.')[-1] + " <= "
            if op == "gt":
                return field.split('.')[-1] + " > "
            if op == "gte":
                return field.split('.')[-1] + " >= "
            if op == "not":
                return "Not "
            return op

        out = out + ' AND '.join(
            ['(' + ' OR '.join([encodeOp(cond) + str(cond[-1]).title().replace('%', '') for cond in conds]) + ')'
             if conds[0] and conds[0][0] != 'not' else
             '(' + ' AND '.join([encodeOp(cond) + str(cond[-1]).title().replace('%', '') for cond in conds]) + ')'
             for conds in conditions.values()])

        if date_conditions:
            date_conditions_sql = ' OR '.join(['(' + cond + ')' for cond in date_conditions.keys()])

            out = out + ' AND ({})'.format(date_conditions_sql) if out else date_conditions_sql

        return out

    def renderConditions(self, conditions, date_conditions=None, cond_sep=None):
        sql = ""
        sep = ' OR '

        inner_sep = ' AND '
        if cond_sep and 'or' in [cond.lower() for cond in list(cond_sep.values())]:
            inner_sep = ' OR '

        def encodeLHS(lhs):

            return lhs

        def encodeRHS(rhs):
            return "'" + str(rhs) + "'"

        def encodeCondition(cond):
            op, lhs, rhs = cond
            nonlocal sep
            sep = ' OR '
            if op == "eq":
                return encodeLHS(lhs) + " = " + encodeRHS(rhs)
            if op == "lk":
                return encodeLHS(lhs) + " like " + encodeRHS(rhs)
            if op == "lt":
                return encodeLHS(lhs) + " < " + encodeRHS(rhs)
            if op == "lte":
                return encodeLHS(lhs) + " <= " + encodeRHS(rhs)
            if op == "gt":
                return encodeLHS(lhs) + " > " + encodeRHS(rhs)
            if op == "gte":
                return encodeLHS(lhs) + " >= " + encodeRHS(rhs)
            if op == "not":
                sep = ' AND '
                return encodeLHS(lhs) + " != " + encodeRHS(rhs)
            return op

        sql = sql + inner_sep.join(
            ['(' + sep.join([encodeCondition(cond) for cond in conds]) + ')'
             if conds[0][0] != 'not' else
             '(' + ' AND '.join([encodeCondition(cond) for cond in conds]) + ')'
             for conds in conditions.values()])

        if date_conditions:
            date_conditions_sql = ' OR '.join(
                ['(' + ' AND '.join([encodeCondition(cond) for cond in conds]) + ')' for conds in
                 date_conditions.values()])

            sql = sql + ' AND ({})'.format(date_conditions_sql) if sql else date_conditions_sql

        return sql

    def processCountConditions(self, qb, agg='COUNT'):
        selects = []

        def encodeSelect(lhs, rhs, agg, encoded_op, and_=None):
            if agg.lower() == 'avg':
                field = "CAST(dbo.{}.{} AS {})".format(qb.tables[0], 'CallLength', 'INT')
            else:
                field = 1

            selects.append(
                ["{agg}(IIF({lhs} {encoded_op} '{rhs}'{and_}, {field}, NULL))".format(lhs=lhs, rhs=rhs, agg=agg,
                                                                                      encoded_op=encoded_op,
                                                                                      field=field,
                                                                                      and_="AND {lhs} {encoded_op} '{rhs}'".format(
                                                                                          lhs=and_[0], rhs=and_[1],
                                                                                          encoded_op=and_[
                                                                                              2]) if and_ else ''),
                 '{agg}_{rhs}'.format(agg=agg, rhs=rhs if rhs else 'NULL')])

            selects.append([
                "CONCAT(IIF({agg}(*)>0,{agg}(IIF({lhs} {encoded_op} '{rhs}'{and_}, {field}, NULL)) * 100 / {agg}(*), 0), '%')".format(
                    lhs=lhs, rhs=rhs, agg=agg,
                    encoded_op=encoded_op,
                    field=field, and_="AND {lhs} {encoded_op} '{rhs}'".format(lhs=and_[0], rhs=and_[1],
                                                                              encoded_op=and_[2]) if and_ else ''),
                '{agg}_{rhs}_PERCENT'.format(agg=agg, rhs=rhs if rhs else 'NULL')])

        def encodeCondition(cond, agg):
            if len(cond) == 2:
                conds = []
                for cond_ in cond:
                    op, lhs, rhs = cond_
                    encoded_op = " = "
                    if op == "eq":
                        encoded_op = " = "
                    if op == "lk":
                        encoded_op = " like "
                    if op == "lt":
                        encoded_op = " < "
                    if op == "lte":
                        encoded_op = " <= "
                    if op == "gt":
                        encoded_op = " > "
                    if op == "gte":
                        encoded_op = " >= "
                    conds.append([lhs, rhs, encoded_op])
                encodeSelect(conds[0][0], conds[0][1], agg, conds[0][2], and_=[conds[1][0], conds[1][1], conds[1][2]])

            else:
                op, lhs, rhs = cond
                encoded_op = " = "
                if op == "eq":
                    encoded_op = " = "
                if op == "lk":
                    encoded_op = " like "
                if op == "lt":
                    encoded_op = " < "
                if op == "lte":
                    encoded_op = " <= "
                if op == "gt":
                    encoded_op = " > "
                if op == "gte":
                    encoded_op = " >= "
                encodeSelect(lhs, rhs, agg, encoded_op)

        other_selects = set()
        for term in qb.conditions:
            other_selects.add(tuple(term))
        for term in other_selects:
            selects.append(["'" + str(term[2]) + "'", term[1]])

        # Handle the group selects
        if qb.date_count_conditions and len(qb.count_conditions) == 4:
            encodeCondition([qb.count_conditions[0], qb.count_conditions[1]], agg)
            encodeCondition([qb.count_conditions[2], qb.count_conditions[3]], agg)
        else:
            for term in qb.count_conditions:
                encodeCondition(term, agg)

        return selects
