import sqlparse as sqlp
import evaluator as ev
import planning as pl
import database as db

sql = 'select p.name, p.surname \
    from people p, cities \
    where p.city = cities.id \
        p.name = ''Joanna'' \
        and cities.name = ''Warszawa''' 

formatted = sqlp.format(sql, keyword_case='upper')
statement = sqlp.parse(formatted)[0]

#statement._pprint_tree()
output = ev.evaluateStatement(statement.tokens)
db = db.createDatabase()
print(pl.naivePlan(output, db))