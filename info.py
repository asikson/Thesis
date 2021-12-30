tablesInfo = dict()
relations = dict()

# tables' structure
tablesInfo['employees'] = ('emp_id', 
    ['first_name',
    'last_name',
    'age',
    'gender'])

tablesInfo['departments'] = ('dept_id',
    ['dept_name',
    'city_id'])

tablesInfo['cities'] = ('city_id',
    ['city_name'])

tablesInfo['dept_emp'] = ('id',
    ['dept_id',
    'emp_id'])

tablesInfo['salaries'] = ('id',
    ['salary',
    'emp_id'])

tablesInfo['dept_manager'] = ('id',
    ['dept_id',
    'emp_id'])

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
    return tablesInfo[tablename][0]

def getTableColumns(tablename):
    return [getTablePk(tablename)] + \
        tablesInfo[tablename][1]

def isForeinKey(tablename1, field1, tablename2, field2):
    return getTablePk(tablename2) == field2 \
        and (tablename2, field2) in relations.keys() \
        and (tablename1, field1) in relations[(tablename2, field2)]

def isColumnInTable(column, tablename):
    return column in getTableColumns(tablename)