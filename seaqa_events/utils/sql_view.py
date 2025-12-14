from seaqa_events.utils.constant import KB_DISPLAY_ALL_COLUMNS


def filter_display_columns(columns):
    return [c for c in (columns or []) if c.get('name') in KB_DISPLAY_ALL_COLUMNS]


def _get_col_by_key_or_name(columns, key=None, name=None):
    if key:
        for c in columns:
            if c.get('key') == key:
                return c
    if name:
        for c in columns:
            if c.get('name') == name:
                return c
    return None


def _escape(v):
    s = str(v)
    s = s.replace("\\", "\\\\").replace("'", "''")
    return s


def _filter_to_sql(columns, item, username=''):
    pred = item.get('filter_predicate', '')
    key = item.get('column_key')
    name = item.get('column_name')
    col = _get_col_by_key_or_name(columns, key, name)
    if not col:
        return ''
    col_name = col.get('name')
    term = item.get('filter_term')
    case_sensitive = item.get('case_sensitive', False)

    if pred == 'include_me':
        term = username

    if pred in ('contains', 'does_not_contain'):
        if not term:
            return ''
        op = 'like' if case_sensitive else 'ilike'
        if pred == 'does_not_contain':
            op = 'not ' + op
        return "`%s` %s '%%%s%%'" % (col_name, op, _escape(term))
    if pred == 'is':
        if term is None or term == '':
            return ''
        return "`%s` = '%s'" % (col_name, _escape(term))
    if pred == 'is_not':
        if term is None or term == '':
            return ''
        return "`%s` <> '%s'" % (col_name, _escape(term))
    if pred == 'equal':
        if term is None or term == '':
            return ''
        return "`%s` = %s" % (col_name, term)
    if pred == 'not_equal':
        if term is None or term == '':
            return ''
        return "`%s` <> %s" % (col_name, term)
    if pred == 'greater':
        return "`%s` > %s" % (col_name, term) if term not in (None, '') else ''
    if pred == 'greater_or_equal':
        return "`%s` >= %s" % (col_name, term) if term not in (None, '') else ''
    if pred == 'less':
        return "`%s` < %s" % (col_name, term) if term not in (None, '') else ''
    if pred == 'less_or_equal':
        return "`%s` <= %s" % (col_name, term) if term not in (None, '') else ''
    if pred in ('is_any_of', 'is_none_of'):
        if not term:
            return ''
        values = term if isinstance(term, list) else [term]
        values = ", ".join(["'%s'" % _escape(v) for v in values])
        op = 'in' if pred == 'is_any_of' else 'not in'
        return "`%s` %s (%s)" % (col_name, op, values)
    if pred == 'is_empty':
        return "`%s` is null" % col_name
    if pred == 'is_not_empty':
        return "`%s` is not null" % col_name
    return ''


def _combine_filters(columns, filters, conjunction, username=''):
    clauses = []
    for item in filters or []:
        sql = _filter_to_sql(columns, item, username=username)
        if sql:
            clauses.append(sql)
    if not clauses:
        return ''
    joiner = ' %s ' % (conjunction or 'And')
    return joiner.join(clauses)


def _filters_sql(columns, view, username=''):
    basic = view.get('basic_filters') or []
    advanced = view.get('filters') or []
    conj = view.get('filter_conjunction') or 'And'
    parts = []
    if basic:
        parts.append(_combine_filters(columns, basic, 'AND', username))
    if advanced:
        parts.append(_combine_filters(columns, advanced, conj, username))
    parts = [p for p in parts if p]
    if not parts:
        return ''
    return 'WHERE ' + ' AND '.join(parts)


def _sorts_sql(columns, view):
    sorts = view.get('sorts') or []
    if not sorts:
        return ''
    clauses = []
    for s in sorts:
        col = _get_col_by_key_or_name(columns, s.get('column_key'), s.get('column_name'))
        if not col:
            continue
        sort_type = s.get('sort_type', 'down')
        order = 'ASC' if sort_type == 'up' else 'DESC'
        clauses.append("`%s` %s" % (col.get('name'), order))
    if not clauses:
        return ''
    return 'ORDER BY ' + ', '.join(clauses)


def view_data_2_sql(table_name, columns, view, username='', start=0, limit=100, include_deleted=False):
    select_cols = ', '.join("`%s`" % c.get('name') for c in (columns or [])) or '*'
    sql = "SELECT %s FROM `%s`" % (select_cols, table_name)
    where_clause = _filters_sql(columns, view or {}, username=username)
    if where_clause:
        sql = "%s %s" % (sql, where_clause)
    if include_deleted:
        if 'WHERE' in sql:
            sql = "%s AND `deleted` = False" % sql
        else:
            sql = "%s WHERE `deleted` = False" % sql
    order_clause = _sorts_sql(columns, view or {})
    if order_clause:
        sql = "%s %s" % (sql, order_clause)
    sql = "%s LIMIT %s, %s" % (sql, start or 0, limit or 100)
    return sql
