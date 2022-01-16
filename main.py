import sql_plugin as sqlp

if __name__ == '__main__':
    queries = sqlp.getQueries()
    statements = [sqlp.formatAndParse(q)
        for q in queries]

    for s in statements:
        #s._pprint_tree()
        print(s)
        sqlp.evaluateStatement(s.tokens)

