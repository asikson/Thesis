import sqlparse as sqlp

# keywords
def isSelectKeyword(token):
    return (token.ttype == sqlp.tokens.Keyword.DML
        and str(token) == 'SELECT')

def isCreateKeyword(token):
    return (token.ttype == sqlp.tokens.Keyword.DDL
        and str(token) == 'CREATE')

def isIndexKeyword(token):
    return (token.ttype == sqlp.tokens.Keyword
        and str(token) == 'INDEX')

def isFromKeyword(token):
    return (token.ttype == sqlp.tokens.Keyword
        and str(token) == 'FROM')

def isWhereKeyword(token):
    return (token.ttype == sqlp.tokens.Keyword
        and str(token) == 'WHERE')

def isOnKeyword(token):
    return (token.ttype == sqlp.tokens.Keyword
        and str(token) == 'ON')


# token types
def isWhitespace(token):
    return token.ttype == sqlp.tokens.Text.Whitespace

def isTokenList(token):
    return isinstance(token, sqlp.sql.TokenList)

def isComparison(token):
    return isinstance(token, sqlp.sql.Comparison)

def isWildcard(token):
    return token.ttype == sqlp.tokens.Wildcard

def isWhere(token):
    return isinstance(token, sqlp.sql.Where)

def isOperator(token):
    return token.ttype == sqlp.tokens.Comparison

def isNumber(token):
    return token.ttype in [sqlp.tokens.Number.Integer,
        sqlp.tokens.Number.Float]

def isFunction(token):
    return isinstance(token, sqlp.sql.Function)

def isParenthesis(token):
    return isinstance(token, sqlp.sql.Parenthesis)


# identifiers
def isIdentifier(token):
    return isinstance(token, sqlp.sql.Identifier)

def isIdentifierList(token):
    return isinstance(token, sqlp.sql.IdentifierList)

def getNamesFromIdentifier(identifier):
    if isTokenList(identifier):
        result = [str(i) for i in identifier.tokens
            if i.ttype == sqlp.tokens.Name]
    else:
        result = str(identifier)
    
    return tuple(result)

def getNamesFromIdentifierList(identifierList):
    return [getNamesFromIdentifier(i) 
        for i in identifierList.get_identifiers()]

def getValueFromIdentifier(identifier):
    return str(identifier)


# table names
def getTablesFromIdentifier(identifier):
    result = [str(i) for i in identifier.tokens
        if i.ttype == sqlp.tokens.Name]

    result += [str(i.tokens[0]) for i in identifier.tokens 
        if isIdentifier(i)]

    return tuple(result)

def getTablesFromIdentifierList(identifierList):
    return [getTablesFromIdentifier(i)
        for i in identifierList.get_identifiers()]


# skipping, finding
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

def findParenthesis(i, tokens):
    j = i + 1
    while (j < len(tokens)
        and not isParenthesis(tokens[j])):
        j += 1
    return j
