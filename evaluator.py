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
    
    # FROM
    i = tt.skipWhitespaces(i, tokens)
    if not tt.isFromKeyword(tokens[i]):
        return -1 
    
    # TABLES
    i = tt.skipWhitespaces(i, tokens) 
    if tt.isIdentifier(tokens[i]): 
        pairs = tt.getTablesFromId(tokens[i])
    elif tt.isIdentifierList(tokens[i]):
        pairs = tt.getTablesFromIdList(tokens[i])
    else:
        return -1

    tables = [p[0] for p in pairs]
    renames = []
    for p in pairs:
        renames.append(ra.Rename(p[1], p[0]))

    # WHERE
    i = tt.skipWhitespaces(i, tokens) 
    if tt.isWhere(tokens[i]):
        predicates = evaluateWhere(tokens[i].tokens)
    else:
        return -1

    return ra.Projection(fields, ra.Selection(predicates, ra.CrossProduct(tables))), renames
    
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
    
    i = tt.findIdentifiers(i, tokens)
    right = tt.getNamesFromId(tokens[i])

    return ra.Predicate(left, right)
