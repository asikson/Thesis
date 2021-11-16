import sqlparse as sqlp
import evaluator as ev

sql = 'select p.name, p.surname \
    from people p, cities c\
    where people.city = cities.id \
        people.name = ''Joanna'' \
        and city.name = ''Warszawa'''

formatted = sqlp.format(sql, keyword_case='upper')
statement = sqlp.parse(formatted)[0]

#statement._pprint_tree()
result, renames = ev.evaluateStatement(statement.tokens)
print(result)
for r in renames:
    print(r)