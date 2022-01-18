from bsddb3 import db

class DbPlugin:
    def __init__(self, tablename):
        self.filename = 'database/' + tablename
        self.data = db.DB()

    def createTable(self, stream):
        self.data.open(self.filename, dbtype=db.DB_HASH, flags=db.DB_CREATE)
        for key, vals in stream:
            self.data.put(self.encodeKey(key),
                self.encodeValues(vals))
        self.close()

    def tableIterator(self):
        cursor = self.data.cursor()

        rec = cursor.first()
        while rec:
            yield rec
            rec = cursor.next()
        self.close()

    def printTable(self):
        for rec in self.tableIterator():
            print(rec)

    def encodeKey(self, key):
        return bytes(str(key), 'utf-8')

    def encodeValues(self, values):
        values = list(map(str, values))
        return '\0'.join(values)

    def decodeKey(self, key):
        return key.decode('utf-8')

    def decodeValues(self, rec):
        return rec.decode('utf-8').split('\0')

    def decodePair(self, pair):
        key, values = pair
        return [self.decodeKey(key)] + self.decodeValues(values)

    def keyFromPair(self, pair):
        key, _ = pair
        return self.decodeKey(key)

    def getValuesByKey(self, key):
        encodedKey = self.encodeKey(key)
        rec = self.data.get(encodedKey)

        if rec is None:
            return -1

        return [key] + self.decodeValues(rec)

    def getValuesByIndexKey(self, keys):
        encodedKey = self.encodeKey(self.encodeValues(keys))
        values = self.data.get(encodedKey)

        if values is None:
            return -1

        return self.decodeValues(values)

    def open(self):
        self.data.open(self.filename, dbtype=db.DB_HASH, flags=db.DB_DIRTY_READ)
    
    def close(self):
        self.data.close()