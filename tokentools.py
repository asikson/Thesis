import sqlparse as sqlp

# token recognizing
def isWhitespace(token):
    return token.ttype == sqlp.tokens.Text.Whitespace

def isSelectKeyword(token):
    return (token.ttype == sqlp.tokens.Keyword.DML
        and str(token) == 'SELECT')

def isIdentifier(token):
    return isinstance(token, sqlp.sql.Identifier)

def isIdentifierList(token):
    return isinstance(token, sqlp.sql.IdentifierList)

def isComparison(token):
    return isinstance(token, sqlp.sql.Comparison)

def isWildcard(token):
    return token.ttype == sqlp.tokens.Wildcard

def isFromKeyword(token):
    return (token.ttype == sqlp.tokens.Keyword
        and str(token) == 'FROM')

def isWhere(token):
    return isinstance(token, sqlp.sql.Where)

def isWhereKeyword(token):
    return (token.ttype == sqlp.tokens.Keyword
        and str(token) == 'WHERE')

def isTokenList(token):
    return isinstance(token, sqlp.sql.TokenList)

def isOperator(token):
    return token.ttype == sqlp.tokens.Comparison

def isNumber(token):
    return token.ttype in [sqlp.tokens.Number.Integer,
        sqlp.tokens.Number.Float]

# skipping and finding
def skipWhitespaces(i, tokens):
    j = i + 1
    while (j < len(tokens)
        and isWhitespace(tokens[j])):
        j += 1
    return j

def findComparisons(i, tokens):
    j = i + 1
    while (j < len(tokens) 
        and not isComparison(tokens[j])):
        j += 1
    return j

def findIdentifiers(i, tokens):
    j = i + 1
    while (j < len(tokens) 
        and not isIdentifier(tokens[j])):
        j += 1
    return j

def findOperator(i, tokens):
    j = i + 1
    while (j < len(tokens) 
        and not isOperator(tokens[j])):
        j += 1
    return j

def findIdentifiersOrNumbers(i, tokens):
    j = i + 1
    while (j < len(tokens) 
        and not isIdentifier(tokens[j])
        and not isNumber(tokens[j])):
        j += 1
    return j

# getting names
def getNamesFromId(identifier):
    if isTokenList(identifier):
        return tuple([str(i)
            for i in identifier.tokens if i.ttype == sqlp.tokens.Name])
    else:
        return tuple(str(identifier))

def getNamesFromIdList(identifierList):
    return [getNamesFromId(i) for i in identifierList.get_identifiers()]

def getTablesFromId(identifier):
    return tuple(
        [str(i) for i in identifier.tokens if i.ttype == sqlp.tokens.Name]
            + [str(i.tokens[0]) for i in identifier.tokens if isIdentifier(i)])

def getTablesFromIdList(identifierList):
    return [getTablesFromId(i) for i in identifierList.get_identifiers()]

def getValueFromIdentifier(identifier):
    return str(identifier)
