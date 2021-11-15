import sqlparse as sqlp
import algebra as ra
import tokentools as tt

def evaluateStatement(tokens):
    # SELECT
    i = tt.skipWhitespaces(-1, tokens)
    if not tt.isSelect(tokens[i]):
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
    if not tt.isFrom(tokens[i]):
        return -1 
    
    # TABLES
    i = tt.skipWhitespaces(i, tokens) 
    if tt.isIdentifier(tokens[i]): 
        tables = tt.getNamesFromId(tokens[i])
    elif tt.isIdentifierList(tokens[i]):
        tables = tt.getNamesFromIdList(tokens[i])
    else:
        return -1

    # WHERE
    i = tt.skipWhitespaces(i, tokens) 
    if tt.isWhere(tokens[i]):
        predicates = evaluateWhere(tokens[i].tokens)
    else:
        return -1

    return ra.Projection(fields, ra.Selection(predicates, ra.CrossProduct(tables)))
    
def evaluateWhere(tokens):
    predicates = []
    i = 0
    if not tt.isWhereKeyword(tokens[i]):
        return -1

    # evaluate comparisons
    i = tt.findComparisons(i, tokens)
    while i < len(tokens):
        predicates.append(evaluateComparison(tokens[i].tokens))
        i = tt.findComparisons(i, tokens)
    
    return predicates

def evaluateComparison(tokens):
    i = tt.findIdentifiers(-1, tokens)
    names1 = tt.getNamesFromId(tokens[i])
    if len(names1) != 2:
        return -1
    else:
        table1 = names1[0]
        field1 = names1[1]
    
    i = tt.findIdentifiers(i, tokens)
    names2 = tt.getNamesFromId(tokens[i])
    if len(names2) == 1:
        value = names2[0]
        table2 = None
        field2 = None
    elif len(names2) == 2:
        table2 = names2[0]
        field2 = names2[1]
        value = None
    else:
        return -1

    return ra.Predicate(table1, field1, table2, field2, value)
