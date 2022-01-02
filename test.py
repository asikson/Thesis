import sqlparse as sqlp
import evaluator as ev
import planning as pl

sql = 'select e.last_name, d.dept_name, \
        c.city_name, s.salary \
    from employees e, dept_emp de, \
        departments d, cities c, salaries s \
    where e.emp_id = de.emp_id \
        and de.dept_id = d.dept_id \
        and d.city_id = c.city_id \
        and e.gender = ''F'' \
        and e.emp_id = s.emp_id \
        and s.salary > 8000'

formatted = sqlp.format(sql, keyword_case='upper')
statement = sqlp.parse(formatted)[0]

#statement._pprint_tree()

output = ev.evaluateStatement(statement.tokens)

plan = pl.Plan(output, None)
plan.estimation()