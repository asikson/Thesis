import db_plugin as dbp
import struct_plugin as sp
from math import inf
from scipy.stats import chisquare
from json import load, dumps

def createStatistics(tablename, fields):
    stat = Statistics(tablename, fields)

def getStatParam(paramName):
    with open('params/stats_params.txt') as f:
            params = load(f)
    if paramName in params.keys():
        return params[paramName]
    else:
        assert(False), "Parameter not found"

class Statistics:
    def __init__(self, tablename, fields):
        self.tablename = tablename
        self.plugin = dbp.DbPlugin(self.tablename)
        self.columns = sp.getTableColumns(tablename)

        # gathered info
        self.tablesize = None
        self.histograms = dict()
        self.mins = dict()
        self.maxes = dict()
        self.diffValues = dict()
        self.uniform = dict()

        self.gatherInfo(fields)
        self.saveStatsToFile()
        self.saveHistogramsToFiles()

    def gatherInfo(self, fields):
        fieldTypes = [sp.getFieldType(self.tablename, f) for f in fields]
        self.tablesize = 0
        visited = dict()
        valuesForHist = dict()

        for t, f in zip(fieldTypes, fields):
            self.diffValues[f] = 0
            visited[f] = set()
            if t == int:
                valuesForHist[f] = []
                self.mins[f] = inf
                self.maxes[f] = -1 * inf

        for rec in self.plugin.tableIterator():
            self.tablesize += 1
            values = self.filterValues(rec, fields)
            triples = list(zip(fields, fieldTypes, values))

            for f, t, v in triples:
                if v not in visited[f]:
                    self.diffValues[f] += 1
                    visited[f].add(v)
                if t == int:
                    v = int(v)
                    if not sp.isTablesPk(self.tablename, f):
                        valuesForHist[f].append(v)
                    if v > self.maxes[f]:
                        self.maxes[f] = v 
                    elif v < self.mins[f]:
                        self.mins[f] = v

        for f, ls in valuesForHist.items():
            self.histograms[f] = self.createHistogram(f, ls)
            self.uniform[f] = self.histograms[f].checkUniform()

    def createHistogram(self, fieldname, values):
        min, max = self.mins[fieldname], self.maxes[fieldname]
        width = (max - min) / getStatParam('numOfBuckets')

        histogram = Histogram(min, max, width)
        for val in values:
            histogram.addValue(val)
        
        return histogram

    def filterValues(self, rec, fields):
        idx = [self.columns.index(f) for f in fields]
        values = self.plugin.decodePair(rec)

        return [values[i] for i in idx]

    def saveHistogramsToFiles(self):
        path = 'stats/histograms/'
        for field, hist in self.histograms.items():
            filename = '{0}_{1}.txt'.format(
                self.tablename,
                field)
            f = open(path + filename, 'w+')
            f.write(hist.toJson())

    def createStatDict(self):
        result = dict()
        result['tablesize'] = self.tablesize
        for f, v in self.mins.items():
            key = '{0}.min'.format(f)
            result[key] = v
        for f, v in self.maxes.items():
            key = '{0}.max'.format(f)
            result[key] = v
        for f, v in self.diffValues.items():
            key = '{0}.diff'.format(f)
            result[key] = v
        for f, v in self.uniform.items():
            key = '{0}.uniform'.format(f)
            result[key] = 'T' if v else 'F'

        return result

    def saveStatsToFile(self):
        path = 'stats/{0}.txt'.format(self.tablename)
        f = open(path, 'w+')
        toJson = dumps(self.createStatDict())
        f.write(toJson)

class Histogram:
    def __init__(self, min, max, width):
        self.min = min
        self.max = max
        self.interval = max - min
        self.width = width
        self.buckets = self.createBuckets()

    def createBuckets(self):
        buckets = []
        l = self.min - 0.5 
        r = self.min + self.width

        while r < self.max:
            buckets.append(Bucket(l, r))
            l += self.width
            r += self.width
        buckets.append(Bucket(l, r))

        return buckets

    def addValue(self, value):
        for b in self.buckets:
            if value < b.right:
                b.bump()
                break

    def checkUniform(self):
        observations = [b.count for b in self.buckets]
        _, p = chisquare(observations)
        return p >= 0.05

    def toJson(self):
        result = dict()
        for b in self.buckets:
            key, val = b.writeFormat()
            result[key] = val 
        
        return dumps(result)

class Bucket:
    def __init__(self, left, right):
        self.left = left
        self.right = right
        self.count = 0

    def bump(self):
        self.count += 1

    def writeFormat(self):
        key = '{0}-{1}'.format(
            str(self.left),
            str(self.right))
        return key, self.count


def getStatDict(tablename):
    with open('stats/{0}.txt'.format(tablename)) as f:
        statsDict = load(f)
    return statsDict

def getHistDict(tablename, fieldname):
    with open('stats/histograms/{0}_{1}.txt'.format(
        tablename, fieldname)) as f:
        histDict = load(f)
    result = dict()
    for k, v in histDict.items():
        pair = tuple(map(float, k.split('-')))
        result[pair] = v

    return result

def getTablesize(tablename):
    stat = getStatDict(tablename)
    return stat['tablesize']

def getDiffValues(tablename, fieldname):
    stat = getStatDict(tablename)
    key = '{0}.diff'.format(fieldname)
    
    return stat[key]

def getMinMax(tablename, fieldname):
    stat = getStatDict(tablename)
    keyMin = '{0}.min'.format(fieldname)
    keyMax = '{0}.max'.format(fieldname)

    return stat[keyMin], stat[keyMax]

def getHistFactorForField(tablename, fieldname, value):
    min, max = getMinMax(tablename)
    if value < min or value > max:
        return 0
    stat = getStatDict(tablename)
    if stat['{0}.uniform'.format(fieldname)] == 'T':
        return 1 / stat['{0}.diff'.format(fieldname)]
    else:
        hist = getHistDict(tablename, fieldname)
        for k, v in hist.items():
            min, max = k
            if value < max:
                return v / (max - min)

def countHistSmaller(tablename, fieldname, value):
    sum = 0
    hist = getHistDict(tablename, fieldname)
    for k, v in hist.items():
            min, max = k
            if value > max:
                sum += v
            else:
                sum += ((value - min) / (max - min)) * v
                return sum
    return getTablesize(tablename)

def countHistLarger(tablename, fieldname, value):
    return getTablesize(tablename) - \
        countHistSmaller(tablename, fieldname, value)

def getInequalityFactor(tablename, fieldname, value, op):
    value = int(value)
    stat = getStatDict(tablename)
    if stat['{0}.uniform'.format(fieldname)] == 'T':
        min, max = getMinMax(tablename, fieldname)
        if value < min or value > max:
            return 0
        interval = max - min
        if op == '<' or op == '<=':
            return (value - min) / interval
        elif op == '>' or op == '>=':
            return (max - value) / interval
        else:
            return -1
    else:
        tablesize = getTablesize(tablename)
        if op == '<' or op == '<=':
            smaller = countHistSmaller(tablename, fieldname, value)
            return smaller / tablesize
        elif op == '>' or op == '>=':
            larger = countHistLarger(tablename, fieldname, value)
            return larger / tablesize
        else:
            return -1


def reductionFactor(predicate):
    # predicates with values
    if predicate.withValue:
        tablename, field, op, val = predicate.split()

        # finding specific values
        if op == '=':
            # primary keys
            if sp.isTablesPk(tablename, field):
                return 1.0 / getTablesize(tablename)
            # other fields
            else:
                t = sp.getFieldType(tablename, field)
                # string fields
                if t == str:
                    return 1.0 / getDiffValues(tablename, field)
                # numbers 
                elif t == int:
                    value = int(val)
                    return getHistFactorForField(tablename, field, value)
                else:
                    return - 1
        # inequalities
        else:
            return getInequalityFactor(tablename, field, val, op)

    # predicates comparing fields
    else:
        tname1, f1, op, tname2, f2 = predicate.split()
        # foreign keys
        if sp.isForeinKey(tname1, f1, tname2, f2):
            return 1.0 / getTablesize(tname2)
        elif sp.isForeinKey(tname2, f2, tname1, f1):
            return 1.0 / getTablesize(tname1)
        # other fields - arbitraryFactor
        else:
            return getStatParam('arbitraryFactor')