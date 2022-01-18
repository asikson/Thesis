from sqlparse import format, parse
import tokentools as tools
import sql_output as sqlout
import index

def getQueries():
    with open('queries.txt') as f:
        content = f.read()
        queries = content.split("\n\n")
        queries = list(map(
            lambda q: q.replace('\n', ' '),
            queries))
        
        return queries

def formatAndParse(query):
    formatted = format(query, keyword_case='upper')
    return parse(formatted)[0]

def evaluateStatement(tokens):
    idx = tools.skipWhitespaces(-1, tokens)
    if tools.isSelectKeyword(tokens[idx]):
        idx = tools.skipWhitespaces(idx, tokens)

        # fields
        fields = getFields(tokens[idx])
        fields = sqlout.formatFields(fields)

        idx = tools.skipWhitespaces(idx, tokens)
        assert(tools.isFromKeyword(tokens[idx])), 'No FROM keyword'
        idx = tools.skipWhitespaces(idx, tokens)

        # tables 
        tables = getTables(tokens[idx])
        tables = sqlout.formatTables(tables)

        idx = tools.skipWhitespaces(idx, tokens)
        if idx < len(tokens):
            # where
            assert(tools.isWhere(tokens[idx])), 'No WHERE keyword'
            
            # predicates
            predicates = getPredicates(tokens[idx].tokens)
        else:
            predicates = []

        fields, predicates = translateAliases(fields, tables, predicates)
        return sqlout.formatOutput(fields, predicates, tables)

    elif tools.isCreateKeyword(tokens[idx]):
        idx = tools.skipWhitespaces(idx, tokens)
        assert(tools.isIndexKeyword(tokens[idx])), 'No INDEX keyword'
        idx = tools.skipWhitespaces(idx, tokens)

        idx_name = tools.getNamesFromIdentifier(tokens[idx])[0]
        idx = tools.skipWhitespaces(idx, tokens)
        assert(tools.isOnKeyword(tokens[idx]))
        idx = tools.skipWhitespaces(idx, tokens)
        assert(tools.isFunction(tokens[idx]))

        tablename, fields = getIndex(tokens[idx].tokens)
        idx = index.Index(tablename, fields, idx_name)
        return idx
    else:
        assert(False), 'Unknown statement'

def getFields(token):
    if tools.isIdentifier(token):
        return [tools.getNamesFromIdentifier(token)]
    elif tools.isIdentifierList(token):
        return tools.getNamesFromIdentifierList(token)
    elif tools.isWildcard(token):
        return []
    else:
        assert(False), 'Not recognized fields in query'

def getTables(token):
    if tools.isIdentifier(token):
        return [tools.getTablesFromIdentifier(token)]
    elif tools.isIdentifierList(token):
        return tools.getTablesFromIdentifierList(token)
    else:
        assert(False), 'Not recognized tables in query'

def getPredicates(tokens):
    idx = tools.skipWhitespaces(-1, tokens)
    assert(tools.isWhereKeyword(tokens[idx]))

    predicates = []
    idx = tools.findComparisons(idx, tokens)
    while idx < len(tokens):
        pred = comparison2Predicate(tokens[idx].tokens)
        predicates.append(pred)
        idx = tools.findComparisons(idx, tokens)

    return predicates

def comparison2Predicate(tokens):
    idx = tools.findIdentifiers(-1, tokens)
    left = tools.getNamesFromIdentifier(tokens[idx])
    left = sqlout.formatField(left)

    idx = tools.findOperator(idx, tokens)
    op = tools.getValueFromIdentifier(tokens[idx])
    assert(checkOperator(op))

    idx = tools.findIdentifiersOrNumbers(idx, tokens)
    if tools.isIdentifier(tokens[idx]):
        right = tools.getNamesFromIdentifier(tokens[idx])
        if len(right) == 1:
            right = right[0]
        else:
            right = sqlout.formatField(right)
    elif tools.isNumber(tokens[idx]):
        right = float(tools.getValueFromIdentifier(tokens[idx]))

    return sqlout.formatPredicate(left, right, op)

def checkOperator(op):
    return op in ['=', '<', '>', '!=', '<=', '>=']

def translateAliases(fields, tables, predicates):
    aliasDict = dict()
    for t in tables:
        aliasDict[t.alias] = t.name
    for f in fields:
        f.tablename = aliasDict[f.tablename]
    for p in predicates:
        p.left.tablename = aliasDict[p.left.tablename]
        if not p.withValue:
            p.right.tablename = aliasDict[p.right.tablename]

    return fields, predicates

def getIndex(tokens):
    idx = tools.findIdentifiers(-1, tokens)
    tablename = tools.getNamesFromIdentifier(tokens[idx])[0]
    idx = tools.findParenthesis(idx, tokens)
    fields = getIndexFields(tokens[idx].tokens)

    return tablename, fields

def getIndexFields(tokens):
    idx = tools.findIdentifiersOrLists(-1, tokens)

    if tools.isIdentifier(tokens[idx]):
        return [tools.getNamesFromIdentifier(tokens[idx])[0]]
    elif tools.isIdentifierList(tokens[idx]):
        return list(map(
            lambda t: t[0],
            tools.getNamesFromIdentifierList(tokens[idx])))
    else:
        assert(False)