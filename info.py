tablesInfo = dict()

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
    ['emp_id',
    'dept_id'])

tablesInfo['salaries'] = ('id',
    ['salary',
    'emp_id'])

tablesInfo['dept_manager'] = ('id',
    ['dept_id',
    'emp_id'])
    

def getTablePk(tablename):
    return tablesInfo[tablename][0]

def getTableColumns(tablename):
    return [getTablePk(tablename)] + \
        tablesInfo[tablename][1]