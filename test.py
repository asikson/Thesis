import sqlparse as sqlp
import evaluator as ev
import planning as pl

sql = 'select e.last_name, d.dept_name \
    from employees e, dept_emp de, departments d, \
    cities c, salaries s \
    where e.emp_id = de.emp_id \
        and de.dept_id = d.dept_id \
        and d.city_id = c.city_id \
        and e.gender = ''F'' \
        and e.age > 30 \
        and e.first_name = ''Abubakar''\
        and s.emp_id = e.emp_id \
        and s.salary > 19000'

formatted = sqlp.format(sql, keyword_case='upper')
statement = sqlp.parse(formatted)[0]

#statement._pprint_tree()

output = ev.evaluateStatement(statement.tokens)

plan = pl.Plan(output, None)
best = plan.bestPlans(1)

for p in best:
    pl.executePlan(p)
