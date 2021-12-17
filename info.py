tablesInfo = dict()
tablesInfo['students'] = ('id', ['name', 'surname', 'gender', 'age', 'city'])
tablesInfo['cities'] = ('id', ['name'])
tablesInfo['teachers'] = ('id', ['name', 'surname', 'city'])

def getTablePk(tablename):
    return tablesInfo[tablename][0]

def getTableColumns(tablename):
    return [getTablePk(tablename)] + \
        tablesInfo[tablename][1]