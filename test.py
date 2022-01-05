import sqlparse as sqlp
import evaluator as ev
import planning as pl

sql = 'select e.last_name, d.dept_name \
    from employees e, dept_emp de, departments d \
    where e.emp_id = de.emp_id \
        and de.dept_id = d.dept_id \
        and e.gender = ''F'' \
        and e.age > 40'

formatted = sqlp.format(sql, keyword_case='upper')
statement = sqlp.parse(formatted)[0]

#statement._pprint_tree()

output = ev.evaluateStatement(statement.tokens)

plan = pl.Plan(output, None)
for p in plan.bestPlans(3):
    pl.executePlan(p)
