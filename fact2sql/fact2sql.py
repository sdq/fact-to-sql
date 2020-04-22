import json

# SELECT [DISTINCT|ALL ] { * | [fieldExpression [AS newName]} FROM tableName [alias] [WHERE condition][GROUP BY fieldName(s)]  [HAVING condition] ORDER BY fieldName(s)

def fact2sql(factjson, table):
    fact = json.loads(factjson)
    clause = ""
    for subspace in fact['subspace']:
        if clause == "":
            clause += "WHERE"
        else:
            clause += " AND"
        clause += " %s='%s'"%(subspace['field'], subspace['value'])

    if fact['type'] == 'value':
        measure = fact['measure'][0]['field']
        aggregate = fact['measure'][0]['aggregate']
        sql = "SELECT %s(%s) FROM %s %s"%(agg(aggregate),measure, table, clause)
        return sql
    elif fact['type'] == 'difference':
        measure = fact['measure'][0]['field']
        aggregate = fact['measure'][0]['aggregate']
        breakdown = fact['breakdown'][0]
        if clause == "":
            clause += "WHERE"
        else:
            clause += " AND"
        clause += " (%s='%s'"%(fact['focus'][0]['field'], fact['focus'][0]['value'])
        clause += " OR %s='%s')"%(fact['focus'][1]['field'], fact['focus'][1]['value'])
        sql = "SELECT %s(%s), %s FROM %s %s GROUP BY %s"%(agg(aggregate), measure, breakdown, table, clause, breakdown)
        return sql
    elif fact['type'] == 'proportion':
        measure = fact['measure'][0]['field']
        aggregate = fact['measure'][0]['aggregate']
        breakdown = fact['breakdown'][0]
        sql = "SELECT %s(%s), %s FROM %s %s GROUP BY %s"%(agg(aggregate), measure, breakdown, table, clause, breakdown)
        return sql
    elif fact['type'] == 'trend':
        measure = fact['measure'][0]['field']
        aggregate = fact['measure'][0]['aggregate']
        breakdown = fact['breakdown'][0]
        sql = "SELECT %s(%s), %s FROM %s %s GROUP BY %s ORDER BY %s ASC"%(agg(aggregate), measure, breakdown, table, clause, breakdown, breakdown)
        return sql
    elif fact['type'] == 'categorization':
        breakdown = fact['breakdown'][0]
        sql = "SELECT COUNT(*), %s FROM %s %s GROUP BY %s"%(breakdown, table, clause, breakdown)
        return sql
    elif fact['type'] == 'distribution':
        measure = fact['measure'][0]['field']
        aggregate = fact['measure'][0]['aggregate']
        breakdown = fact['breakdown'][0]
        sql = "SELECT %s(%s), %s FROM %s %s GROUP BY %s"%(agg(aggregate), measure, breakdown, table, clause, breakdown)
        return sql
    elif fact['type'] == 'rank':
        measure = fact['measure'][0]['field']
        aggregate = fact['measure'][0]['aggregate']
        breakdown = fact['breakdown'][0]
        sql = "SELECT %s(%s) AS rank_measure, %s FROM %s %s GROUP BY %s ORDER BY rank_measure DESC"%(agg(aggregate), measure, breakdown, table, clause, breakdown)
        return sql
    elif fact['type'] == 'association':
        measure1 = fact['measure'][0]['field']
        aggregate1 = fact['measure'][0]['aggregate']
        measure2 = fact['measure'][1]['field']
        aggregate2 = fact['measure'][1]['aggregate']
        breakdown = fact['breakdown'][0]
        sql = "SELECT %s(%s), %s(%s), %s FROM %s %s GROUP BY %s"%(agg(aggregate1), measure1, agg(aggregate2), measure2, breakdown, table, clause, breakdown)
        return sql
    elif fact['type'] == 'extreme':
        measure = fact['measure'][0]['field']
        aggregate = fact['measure'][0]['aggregate']
        breakdown = fact['breakdown'][0]
        sql = "SELECT %s(%s), %s FROM %s GROUP BY %s"%(agg(aggregate), measure, breakdown, table, breakdown)
        return sql
    elif fact['type'] == 'outlier':
        measure = fact['measure'][0]['field']
        aggregate = fact['measure'][0]['aggregate']
        breakdown = fact['breakdown'][0]
        sql = "SELECT %s(%s), %s FROM %s GROUP BY %s"%(agg(aggregate), measure, breakdown, table, breakdown)
        return sql
    else:
        return ""

def agg(aggregate):
    if aggregate == 'sum':
        return 'SUM'
    elif aggregate == 'max':
        return 'MAX'
    elif aggregate == 'min':
        return 'MIN'
    elif aggregate == 'avg':
        return 'AVG'
    elif aggregate == 'count':
        return 'COUNT'
    else:
        return 'MAX'