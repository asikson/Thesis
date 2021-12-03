import sqlparse as sqlp
import evaluator as ev
import planning as pl
import database as db

sql = 'select * \
    from people p, cities c \
    where p.city = c.id \
        and p.name != ''Joanna''' 

formatted = sqlp.format(sql, keyword_case='upper')
statement = sqlp.parse(formatted)[0]

#statement._pprint_tree()
output = ev.evaluateStatement(statement.tokens)

db = db.createDatabase()

plan = pl.naivePlan(output, db)
print(pl.executePlan(plan))