from bsddb3 import db

class DbPlugin:
    def __init__(self, filename):
        self.filename = filename
        self.data = db.DB()

    def createTable(self, stream):
        self.data.open('database/' + self.filename,
            dbtype=db.DB_HASH, flags=db.DB_CREATE)

        for key, vals in stream:
            self.data.put(self.formatKey(key),
                self.formatValues(vals))

        self.data.close()

    def formatKey(self, key):
        return bytes(str(key), 'utf-8')

    def formatValues(self, values):
        values = list(map(str, values))
        return '\0'.join(values)