import sqlparse as sqlp
import evaluator as ev

sql = 'select p.name, p.surname \
    from people p, cities c\
    where p.city = c.id \
        p.name = ''Joanna'' \
        and c.name = ''Warszawa'''

formatted = sqlp.format(sql, keyword_case='upper')
statement = sqlp.parse(formatted)[0]

#statement._pprint_tree()
result = ev.evaluateStatement(statement.tokens)
print(result)