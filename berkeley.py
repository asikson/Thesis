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
    data.open(tablename, dbtype=db.DB_HASH, flags=db.DB_DIRTY_READ)
    key = bytes(val, 'utf-8')
    val = data.get(key)
    data.close()
    if val is None:
        return -1

    return getValuesFromRecord((key, val))