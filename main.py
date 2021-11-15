import sqlparse as sqlp
import evaluator as ev

sql = 'select name, surname \
    from people, cities \
    where people.city = cities.id \
        people.name = ''Joanna'' \
        and city.name = ''Warszawa'''

formatted = sqlp.format(sql, keyword_case='upper')
statement = sqlp.parse(formatted)[0]

#statement._pprint_tree()
result = ev.evaluateStatement(statement.tokens)
print(result.toString())