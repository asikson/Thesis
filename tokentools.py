import sqlparse as sqlp

# token recognizing
def isWhitespace(token):
    return token.ttype == sqlp.tokens.Text.Whitespace

def isSelectKeyword(token):
    return (token.ttype == sqlp.tokens.Keyword.DML
        and token._get_repr_value() == 'SELECT')

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
        and token._get_repr_value() == 'FROM')

def isWhere(token):
    return isinstance(token, sqlp.sql.Where)

def isWhereKeyword(token):
    return (token.ttype == sqlp.tokens.Keyword
        and token._get_repr_value() == 'WHERE')

def isTokenList(token):
    return isinstance(token, sqlp.sql.TokenList)

# skipping and finding
def skipWhitespaces(i, tokens):
    j = i + 1
    while isWhitespace(tokens[j]):
        j += 1
    return j

def findComparisons(i, tokens):
        j = i + 1
        while j < len(tokens) and not isComparison(tokens[j]):
            j += 1
        return j

def findIdentifiers(i, tokens):
        j = i + 1
        while j < len(tokens) and not isIdentifier(tokens[j]):
            j += 1
        return j


# getting names
def getNamesFromId(identifier):
    if isTokenList(identifier):
        return [i._get_repr_value() for i in identifier.tokens 
            if i.ttype == sqlp.tokens.Name]
    else:
        return [identifier._get_repr_value()]

def getNamesFromIdList(identifierList):
    return [name for i in identifierList.get_identifiers()
        for name in getNamesFromId(i)]
