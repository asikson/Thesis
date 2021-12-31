from sqlparse.tokens import String

tablesInfo = dict()
relations = dict()

# tables' structure
tablesInfo['employees'] = (('emp_id', int), 
    [('first_name', str),
    ('last_name', str),
    ('age', int),
    ('gender', str)])

tablesInfo['departments'] = (('dept_id', int),
    [('dept_name', str),
    ('city_id', int)])

tablesInfo['cities'] = (('city_id', int),
    [('city_name', str)])

tablesInfo['dept_emp'] = (('id', int),
    [('dept_id', int),
    ('emp_id', int)])

tablesInfo['salaries'] = (('id', int),
    [('salary', int),
    ('emp_id', int)])

tablesInfo['dept_manager'] = (('id', int),
    [('dept_id', int),
    ('emp_id', int)])

# relations
relations[('employees', 'emp_id')] = [
    ('dept_emp', 'emp_id'),
    ('dept_manager', 'emp_id'),
    ('salaries', 'emp_id')]

relations[('departments', 'dept_id')] = [
    ('dept_emp', 'dept_id')]

relations[('cities', 'city_id')] = [
    ('departments', 'city_id')]


def getTablePk(tablename):
    return tablesInfo[tablename][0][0]

def getTableColumns(tablename):
    return [getTablePk(tablename)] + \
        list(map(lambda f: f[0], tablesInfo[tablename][1]))

def isForeinKey(tablename1, field1, tablename2, field2):
    return isTablesPk(tablename2, field2) \
        and (tablename2, field2) in relations.keys() \
        and (tablename1, field1) in relations[(tablename2, field2)]

def isColumnInTable(column, tablename):
    return column in getTableColumns(tablename)

def isTablesPk(tablename, field):
    return getTablePk(tablename) == field

def getFieldType(tablename, field):
    for f in tablesInfo[tablename][1]:
        if f[0] == field:
            return f[1]
    return None