import output as out
import tokentools as tt

def evaluateStatement(tokens):
    # SELECT
    i = tt.skipWhitespaces(-1, tokens)
    if not tt.isSelectKeyword(tokens[i]):
        return -1

    # FIELDS
    i = tt.skipWhitespaces(i, tokens)
    if tt.isIdentifier(tokens[i]):
        fields = [tt.getNamesFromId(tokens[i])]
    elif tt.isIdentifierList(tokens[i]):
        fields = tt.getNamesFromIdList(tokens[i])
    elif tt.isWildcard(tokens[i]):
        fields = []
    else:
        return -1 

    fields = list(map(lambda f: out.Field(f[1], f[0]), fields))

    # FROM
    i = tt.skipWhitespaces(i, tokens)
    if not tt.isFromKeyword(tokens[i]):
        return -1 
    
    # TABLES
    i = tt.skipWhitespaces(i, tokens) 
    if tt.isIdentifier(tokens[i]): 
        tables = [tt.getTablesFromId(tokens[i])]
    elif tt.isIdentifierList(tokens[i]):
        tables = tt.getTablesFromIdList(tokens[i])
    else:
        return -1

    tables = list(map(lambda t: out.Table(t[0], t[1]) if len(t) > 1
                                else out.Table(t[0], t[0]), tables))

    # WHERE
    i = tt.skipWhitespaces(i, tokens) 
    if i == len(tokens):
        return out.evaluatorOutput(fields, [], tables)
    if tt.isWhere(tokens[i]):
        predicates = evaluateWhere(tokens[i].tokens)
    else:
        return -1

    # aliases' translation
    tablesDict = dict()
    for t in tables:
        tablesDict[t.alias] = t.name
    for f in fields:
        f.tablename = tablesDict[f.tablename]
    for p in predicates:
        p.left.tablename = tablesDict[p.left.tablename]
        p.right.tablename = tablesDict[p.right.tablename]

    return out.evaluatorOutput(fields, predicates, tables)
    
def evaluateWhere(tokens):
    predicates = []
    i = 0
    if not tt.isWhereKeyword(tokens[i]):
        return -1

    i = tt.findComparisons(i, tokens)
    while i < len(tokens):
        predicates.append(evaluateComparison(tokens[i].tokens))
        i = tt.findComparisons(i, tokens)
    
    return predicates

def evaluateComparison(tokens):
    i = tt.findIdentifiers(-1, tokens)
    left = tt.getNamesFromId(tokens[i])
    left = out.Field(left[1], left[0])

    i = tt.findOperator(i, tokens)
    operator = tt.getValueFromIdentifier(tokens[i])
    if operator not in ['=', '<', '>', '!=']:
        return -1
    
    i = tt.findIdentifiersOrNumbers(i, tokens)
    if tt.isIdentifier(tokens[i]):
        right = tt.getNamesFromId(tokens[i])
        if len(right) == 1:
            right = right[0]
        else:
            right = out.Field(right[1], right[0])
    elif tt.isNumber(tokens[i]):
        right = float(tt.getValueFromIdentifier(tokens[i]))

    return out.Predicate(left, right, operator)