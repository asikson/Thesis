import algebra as ra
import tokentools as tt

def evaluateStatement(tokens):
    # SELECT
    i = tt.skipWhitespaces(-1, tokens)
    if not tt.isSelectKeyword(tokens[i]):
        return -1 

    # FIELDS
    i = tt.skipWhitespaces(i, tokens)
    if tt.isIdentifier(tokens[i]):
        fields = tt.getNamesFromId(tokens[i])
    elif tt.isIdentifierList(tokens[i]):
        fields = tt.getNamesFromIdList(tokens[i])
    elif tt.isWildcard(tokens[i]):
        fields = []
    else:
        return -1 

    fields = list(map(lambda f: ra.Field(f[1], f[0]), fields))
    
    # FROM
    i = tt.skipWhitespaces(i, tokens)
    if not tt.isFromKeyword(tokens[i]):
        return -1 
    
    # TABLES
    i = tt.skipWhitespaces(i, tokens) 
    if tt.isIdentifier(tokens[i]): 
        tables = tt.getTablesFromId(tokens[i])
    elif tt.isIdentifierList(tokens[i]):
        tables = tt.getTablesFromIdList(tokens[i])
    else:
        return -1

    tables = list(map(lambda t: ra.Table(t[0], t[1]), tables))

    # WHERE
    i = tt.skipWhitespaces(i, tokens) 
    if i == len(tokens):
        return ra.Projection(fields, ra.CrossProductList(tables))
    if tt.isWhere(tokens[i]):
        predicates = evaluateWhere(tokens[i].tokens)
    else:
        return -1

    return ra.Projection(fields, ra.Selection(predicates, ra.CrossProductList(tables)))
    
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
    left = ra.Field(left[1], left[0])
    
    i = tt.findIdentifiers(i, tokens)
    right = tt.getNamesFromId(tokens[i])
    if len(right) == 1:
        right = right[0]
    else:
        right = ra.Field(right[1], right[0])

    return ra.Predicate(left, right)
