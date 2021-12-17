from bsddb3 import db

def printTable(name):
    for r in tableIterator(name):
        print(r)

def tableIterator(name):
    data = db.DB()
    data.open(name, dbtype=db.DB_HASH, flags=db.DB_DIRTY_READ)
    cursor = data.cursor()
    rec = cursor.first()

    while rec:
        yield rec
        rec = cursor.next()
    data.close()

def getValuesFromRecord(rec):
    separated = rec[1].decode('utf-8').split('\0')
    pk = rec[0].decode('utf-8')
    separated.insert(0, pk)
    return separated

def getValuesByPk(tablename, val):
    data = db.DB()
    data = data.open(tablename, dbtype=db.DB_HASH, flags=db.DB_DIRTY_READ)
    val = bytes(val, 'utf-8')

    return getValuesFromRecord(data.get(val))