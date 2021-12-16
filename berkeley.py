from bsddb3 import db

def printTable(name):
    for r in tableIterator(name):
        print(r)

def tableIterator(name):
    data = db.DB()
    print(name)
    data.open(name, dbtype=db.DB_HASH, flags=db.DB_DIRTY_READ)
    cursor = data.cursor()
    rec = cursor.first()

    while rec:
        yield rec
        rec = cursor.next()
    data.close()

def getValuesFromRecord(rec):
    return rec[1].split('\0').insert()

def getPairFromRecord(rec):
    return rec[0], getValuesFromRecord(rec)

def getValuesByPk(tablename, val):
    data = db.DB()
    data = data.open(tablename, dbtype=db.DB_HASH, flags=db.DB_DIRTY_READ)

    return getValuesFromRecord(data.get(val))