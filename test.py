import sqlparse as sqlp
import evaluator as ev
import planning as pl
import database as db

db = db.createDatabase()

sql = 'select p.name, p.surname, p.age, c.name \
    from people p, cities c \
    where p.city = c.id \
        and p.age > 25'

formatted = sqlp.format(sql, keyword_case='upper')
statement = sqlp.parse(formatted)[0]

#statement._pprint_tree()

output = ev.evaluateStatement(statement.tokens)

plan1 = pl.Plan(output, db, 'cross_all')
plan1.execute()

print()

plan2 = pl.Plan(output, db, 'selection_pushdown')
plan2.execute()

print()
plan3 = pl.Plan(output, db, 'apply_joins')
plan3.execute()