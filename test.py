import sqlparse as sqlp
import evaluator as ev
import planning as pl

sql = 'select s.name, s.surname, s.age, c.name \
    from students s, cities c \
    where s.city = c.id \
        and s.age > 25'

formatted = sqlp.format(sql, keyword_case='upper')
statement = sqlp.parse(formatted)[0]

#statement._pprint_tree()

output = ev.evaluateStatement(statement.tokens)

plan1 = pl.Plan(output, 'cross_all')
plan1.execute()

print()

plan2 = pl.Plan(output, 'selection_pushdown')
plan2.execute()

print()
plan3 = pl.Plan(output, 'apply_joins')
plan3.execute()