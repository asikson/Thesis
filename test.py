import sqlparse as sqlp
import evaluator as ev
import planning as pl

sql = 'select e.emp_id, e.last_name, \
        de.emp_id, de.dept_id, d.dept_id, d.dept_name, \
        c.city_name \
    from employees e, dept_emp de, \
        departments d, cities c \
    where e.emp_id = de.emp_id \
        and de.dept_id = d.dept_id \
        and d.city_id = c.city_id'

formatted = sqlp.format(sql, keyword_case='upper')
statement = sqlp.parse(formatted)[0]

#statement._pprint_tree()

output = ev.evaluateStatement(statement.tokens)

plan = pl.Plan(output, 'apply_joins')
plan.execute()