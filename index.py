import db_plugin as dbp
import struct_plugin as sp

class Index:
    def __init__(self, tablename, fields):
        self.tablename = tablename
        self.fields = fields
        self.filename = '{0}_idx_{1}'.format(
            self.tablename,
            self.fields2Name())
        self.plugin = dbp.DbPlugin(self.filename)
        self.columns = sp.getTableColumns(tablename)

        self.createIndex()

    def fields2Name(self):
        return '_'.join(self.fields)

    def filterValues(self, rec, fields):
        idx = [self.columns.index(f) for f in fields]
        values = self.plugin.decodePair(rec)

        return [values[i] for i in idx]
    
    def indexStream(self):
        index = dict()
        tablePlugin = dbp.DbPlugin(self.tablename)
        for rec in tablePlugin.tableIterator():
            pk = tablePlugin.keyFromPair(rec)
            vals4Key = tuple(self.filterValues(rec, self.fields))
            if vals4Key in index.keys():
                index[vals4Key] += '\0' + str(pk)
            else:
                index[vals4Key] = str(pk)

        for k, v in index.items():
            yield tablePlugin.encodeValues(k), [v]

    def createIndex(self):
        self.plugin.createTable(self.indexStream())