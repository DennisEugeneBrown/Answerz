from collections import defaultdict


class QueryBlock:
    def __init__(self, intent_summary=None):
        self.queryIntent = intent_summary
        self.tables = []
        self.selects = []
        self.joins = []
        self.conditions = []
        self.count_conditions = []
        self.date_count_conditions = False
        self.groups = []
        self.sorts = []
        self.comparators = []
        self.string_operators = []
        self.is_compare = False
        self.is_cross = False
        self.aggregation = None
        self.unions = []
        self.distinct_values_query = None
        self.with_query = None
        self.totals = None
        self.is_total = False
        self.unpivot_selects = []
        self.unpivot_cols = []
        self.groups_to_skip = []
        self.cond_sep = {-1: 'AND'}  # Default
        self.logical_label = False
        self.condition_locs = {}
        self.conditions_by_type = {}
        self.conditions_by_category = {}
        self.date_range_conditions = {}
        self.choice_for_field = {}

    def addTable(self, tableNames, join=None):
        if not isinstance(tableNames, list):
            tableNames = [tableNames]
        for tableName in tableNames:
            if join:
                for join in self.joins:
                    if join[0] == tableName:
                        #
                        # Already have this join. Let's hope it doesnt conflict!
                        #
                        break

                self.joins.append((tableName, join))
            else:
                self.tables.append(tableName)

    def addGroup(self, group_field):
        self.groups.append(group_field)

    def generateConditionalCountSelect(self, conditions):
        from answerz.process.QueryBlockRenderer import QueryBlockRenderer
        qbr = QueryBlockRenderer()
        rendered_condition = qbr.renderConditions(conditions)
        sql = "Count(IIF({}, 1, null))".format(rendered_condition)
        field_name = ' And '.join(
            [('Not ' if cond[0][0] == 'not' else '') + cond[0][-1].replace('%', '') for cond in conditions.values()])
        return [sql, field_name]

    def getAllSelects(self):
        # the order here is intentionally backwards. we pre-end the select
        # This is a way to ensure the array is COPIED and not REFERENCED. as we will be modifying the array
        allSelects = []
        allSelects.extend([group for group in self.groups[:] if
                           group[0] not in self.groups_to_skip and group not in self.selects])
        pivot_condition_selects = []
        if self.conditions_by_category and not self.count_conditions:
            pivot_condition_category = list(self.conditions_by_category.keys())[0]
            pivot_conditions_count = len(self.conditions_by_category[pivot_condition_category])
            other_condition_categories = list(self.conditions_by_category.keys())[:-1]
            for condition in self.conditions_by_category[pivot_condition_category]:
                other_condition_selects = []
                for other_condition_category in other_condition_categories:
                    if len(self.conditions_by_category[other_condition_category]) <= 1:
                        continue
                    for other_condition in self.conditions_by_category[other_condition_category]:
                        other_condition_selects.append(self.generateConditionalCountSelect(
                            {pivot_condition_category: [condition], other_condition_category: [other_condition]}))
                if other_condition_selects:
                    allSelects.extend(other_condition_selects)
                if pivot_conditions_count > 1:
                    pivot_condition_select = self.generateConditionalCountSelect(
                        {pivot_condition_category: [condition]})
                    pivot_condition_selects.append(pivot_condition_select)
                    allSelects.append(pivot_condition_select)
        else:
            pivot_condition_selects = []
            for condition_1, condition_2 in self.date_range_conditions.values():
                pivot_condition_select_1 = self.generateConditionalCountSelect(
                    {'DateRange': [condition_1]})
                pivot_condition_selects.append(pivot_condition_select_1)
                pivot_condition_select_2 = self.generateConditionalCountSelect(
                    {'DateRange': [condition_1]})
                pivot_condition_selects.append(pivot_condition_select_2)
                allSelects.append([condition_1, condition_2])
        allSelects.extend(
            [select for select in self.selects if select[0] not in self.groups_to_skip])
        for ix, select in enumerate(allSelects):
            if select[1] == 'Difference':
                allSelects[ix][0] = pivot_condition_selects[-1][0] + '-' + pivot_condition_selects[0][0]
            if select[1] == 'Difference %':
                allSelects[ix][0] = '(' + pivot_condition_selects[-1][0] + '-' + pivot_condition_selects[0][
                    0] + ')' + '/' + pivot_condition_selects[-1][0] + '*' + '100'
                left = pivot_condition_selects[-1][0]
                right = pivot_condition_selects[0][0]
                allSelects[ix][
                    0] = "CAST(CAST((CAST(({left} - {right}) AS FLOAT) / NULLIF({right}, 0)) * 100.0 AS decimal(10, 2)) AS varchar) + '%'".format(
                    left=left, right=right)
                print(allSelects[ix][0])

        return allSelects

    def merge_conditions_by_type(self, other_conditions):
        conditions_by_type = defaultdict(list)
        for cond_type, vals in self.conditions_by_type.items():
            conditions_by_type[cond_type].extend(vals)
        for cond_type, vals in other_conditions.items():
            conditions_by_type[cond_type].extend(vals)
        return conditions_by_type

    def merge_conditions_by_category(self, other_conditions):
        conditions_by_category = defaultdict(list)
        for cond_category, vals in self.conditions_by_category.items():
            conditions_by_category[cond_category].extend(vals)
        for cond_category, vals in other_conditions.items():
            conditions_by_category[cond_category].extend(vals)
        return conditions_by_category

    def merge(self, qb_other):
        if not qb_other:
            return False
        if qb_other.tables and qb_other.tables != self.tables:
            print("Root table mismatch on Query Block merge")
            print(qb_other.tables)
            print(self.tables)
            print("---")
            return

        # This is naive for now
        self.selects.extend(qb_other.selects)
        self.joins.extend(tuple(qb_other.joins))
        self.conditions.extend(qb_other.conditions)
        self.count_conditions.extend(qb_other.count_conditions)
        self.groups.extend(qb_other.groups)
        self.sorts.extend(qb_other.sorts)
        self.comparators.extend(qb_other.comparators)
        self.is_compare = (self.is_compare or qb_other.is_compare)
        self.aggregation = qb_other.aggregation if qb_other.aggregation else self.aggregation
        self.logical_label = qb_other.logical_label
        self.condition_locs = {**self.condition_locs, **qb_other.condition_locs}
        self.conditions_by_type = self.merge_conditions_by_type(qb_other.conditions_by_type)
        self.conditions_by_category = self.merge_conditions_by_category(qb_other.conditions_by_category)
        self.date_range_conditions = {**self.date_range_conditions, **qb_other.date_range_conditions}
        self.choice_for_field = {**self.choice_for_field, **qb_other.choice_for_field}
        return True
