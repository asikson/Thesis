import db_plugin as dbp
import struct_plugin as sp
import os
import json
from re import compile

class Index:
    def __init__(self, tablename, fields, indexName):
        self.tablename = tablename
        self.fields = fields
        self.filename = self.getFilename(indexName)
        self.plugin = dbp.DbPlugin(self.filename)
        self.columns = sp.getTableColumns(tablename)

    def getFilename(self, indexName):
        if indexName is None:
            return '{0}idx_{1}'.format(
                self.tablename,
                self.fields2Name())
        else:
            return '{0}idx_{1}'.format(
                self.tablename,
                indexName)

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
        self.saveIndexInfo()

    def saveIndexInfo(self):
        path = 'database/indexes.txt'
        if os.path.exists(path):
            m = 'a'
            toWrite = '\n'
        else:
            m = 'w'
            toWrite = ''

        indexDict = dict()
        indexDict['table'] = self.tablename
        indexDict['name'] = self.filename
        for i, f in enumerate(self.fields):
            k = 'f'+ str(i)
            indexDict[k] = f
        
        toWrite += json.dumps(indexDict)
        f = open(path, m)
        f.write(toWrite)
        f.close()

def checkIfIndexExists(tablename, fields):
    path = 'database/indexes.txt'
    if not os.path.exists(path):
        return False

    with open(path) as f:
        for line in f.readlines():
            idxDict = json.loads(line)
            if idxDict['table'] == tablename:
                idxFields = getFieldsFromIdxDict(idxDict)
                idxFields = set(idxFields)
                if checkFields(fields, idxFields):
                    return idxDict['name']
    
    return False

def getFieldsFromIdxDict(idxDict):
    return [v for k, v in idxDict.items()
        if compile('f\d*').match(k)]

def checkFields(fields, fieldSet):
    if len(fields) != len(fieldSet):
        return False
    for f in fields:
        if f not in fieldSet:
            return False
    return True