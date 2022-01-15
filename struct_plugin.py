from json import load
from pydoc import locate

def getTableDict(tablename):
    with open('structure/{0}.txt'.format(tablename)) as f:
        tableDict = load(f)
    return tableDict

def getTableColumns(tablename):
    return list(getTableDict(tablename).keys())

def getFieldType(tablename, fieldname):
    return locate(getTableDict(tablename)[fieldname])

def getTablePk(tablename):
    with open('structure/keys/primary_keys.txt') as f:
        pkDict = load(f)
    return pkDict[tablename]

def isTablesPk(tablename, fieldname):
    return getTablePk(tablename) == fieldname

def isForeinKey(tablename1, field1, tablename2, field2):
    key2Search = tablename1 + '.' + field1
    val2Search = tablename2 + '.' + field2
    with open('structure/keys/foreign_keys.txt') as f:
        fkDict = load(f)

    return (key2Search in fkDict.keys() 
        and fkDict[key2Search] == val2Search
        and isTablesPk(tablename1, field1)) 
