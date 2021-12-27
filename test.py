import sqlparse as sqlp
import evaluator as ev
import planning as pl

sql = 'select * from departments d, cities c \
    where d.city_id = c.city_id'

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