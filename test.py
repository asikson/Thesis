import sqlparse as sqlp
import evaluator as ev
import planning as pl
import database as db

db = db.createDatabase()

sql = 'select p.name, p.surname, c.name \
    from people p, cities c \
    where p.city = c.id '

formatted = sqlp.format(sql, keyword_case='upper')
statement = sqlp.parse(formatted)[0]

#statement._pprint_tree()
output = ev.evaluateStatement(statement.tokens)


plan1 = pl.Plan(output, db, 'naive')
print(plan1.execute())

print('\n')

plan2 = pl.Plan(output, db, 'join')
print(plan2.execute())
