from scipy.stats.stats import HistogramResult
import info
from math import inf
from scipy.stats import chisquare
import db_plugin as dbp

statistics = dict()
numOfBuckets = 20

def getStatistics(tablename):
    return statistics[tablename]

def createStatistics(tablename, fields):
    statistics[tablename] = Statistics(tablename, fields)

def reductionFactor(predicate):
    # predicates with values
    if predicate.withValue:
        tablename, field, op, val = predicate.split()
        stat = getStatistics(tablename)

        # finding specific values
        if op == '=':
            # primary keys
            if info.isTablesPk(tablename, field):
                return 1.0 / stat.getTableSize()
            # other fields
            else:
                t = info.getFieldType(tablename, field)
                # string fields
                if t == str:
                    return 1.0 / stat.getDiffValues(field)
                # numbers 
                elif t == int:
                    value = int(val)
                    return stat.getHistogramFactorForField(field, value)
                else:
                    return - 1
        # inequalities
        else:
            return stat.getInequalityFactor(field, val, op)

    # predicates comparing fields
    else:
        tname1, f1, op, tname2, f2 = predicate.split()
        # foreign keys
        if info.isForeinKey(tname1, f1, tname2, f2):
            stat = getStatistics(tname2)
            return 1.0 / stat.tablesize
        elif info.isForeinKey(tname2, f2, tname1, f1):
            stat = getStatistics(tname1)
            return 1.0 / stat.tablesize
        # other fields
        else:
            return 0.1


class Bucket:
    def __init__(self, left, right):
        self.left = left
        self.right = right
        self.count = 0

    def bump(self):
        self.count += 1

    def __str__(self):
        return '| ({0} - {1}) {2} |'.format(
            self.left, self.right, self.count)

class EquiwidthHistogram:
    def __init__(self, min, max, width):
        self.min = min
        self.max = max
        self.interval = max - min
        self.width = width
        self.buckets = self.createBuckets()
        self.cachedValues = dict()

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
        if value >= self.min:
            for b in self.buckets:
                if value < b.right:
                    b.bump()
                    break

    def factorForValue(self, value):
        if value in self.cachedValues.keys():
            return self.cachedValues[value]
        for b in self.buckets:
            if value < b.right:
                fact = b.count * (1.0 / self.width)
                self.cachedValues[value] = fact
                return fact

    def checkUniformDist(self):
        observations = [b.count for b in self.buckets]
        _, p = chisquare(observations)

        return p >= 0.05

    def printBuckets(self):
        for b in self.buckets:
            print(b)

    def countSmaller(self, value):
        sum, i = 0, 0
        while self.buckets[i].right < value:
            sum += self.buckets[i].count
            i += 1
        sum += (i / self.width) * self.buckets[i].count

        return sum
    
    def countLarger(self, value):
        sum, i = 0, len(self.buckets) - 1
        while self.buckets[i].left > value:
            sum += self.buckets[i].count
            i -= 1
        sum += (i / self.width) * self.buckets[i].count

        return sum

class Statistics:
    def __init__(self, tablename, fields):
        self.tablename = tablename
        self.plugin = dbp.DbPlugin(self.tablename)
        self.columns = info.getTableColumns(tablename)

        # cached info
        self.tablesize = None
        self.histograms = dict()
        self.mins = dict()
        self.maxes = dict()
        self.diffValues = dict()
        self.gatherStatistics(fields)

    def valuesForFields(self, rec, fields):
        idx = [self.columns.index(f) for f in fields]
        values = self.plugin.decodePair(rec)
        
        return [values[i] for i in idx]

    def getMinMax(self, fieldname):
        return self.mins[fieldname], self.maxes[fieldname]

    def createEquiwidthHistogram(self, fieldname, values):
        min, max = self.getMinMax(fieldname)
        width = (max - min) / numOfBuckets

        histogram = EquiwidthHistogram(min, max, width)
        for val in values:
            histogram.addValue(val)
        
        return histogram

    def gatherStatistics(self, fields):
        types = [info.getFieldType(self.tablename, f) for f in fields]
        self.tablesize = 0
        visited = dict()
        valuesForHist = dict()

        for t, f in zip(types, fields):
            self.diffValues[f] = 0
            visited[f] = set()
            if t == int:
                valuesForHist[f] = []
                self.mins[f] = inf
                self.maxes[f] = -1 * inf

        for rec in self.plugin.tableIterator():
            self.tablesize += 1
            values = list(zip(fields, types, self.valuesForFields(rec, fields)))
            
            for f, t, v in values:
                if v not in visited[f]:
                    self.diffValues[f] += 1
                    visited[f].add(v)
                if t == int:
                    v = int(v)
                    valuesForHist[f].append(v)
                    if v > self.maxes[f]:
                        self.maxes[f] = v 
                    elif v < self.mins[f]:
                        self.mins[f] = v

        for f, ls in valuesForHist.items():
            self.histograms[f] = self.createEquiwidthHistogram(f, ls)

    def getTableSize(self):
        return self.tablesize

    def getDiffValues(self, fieldname):
        return self.diffValues[fieldname]

    def getHistogramFactorForField(self, fieldname, value):
        value = int(value)
        histogram = self.histograms[fieldname]
        if histogram.checkUniformDist():
            return 1 / self.getDiffValues(fieldname)
        else:
            return histogram.factorForValue(value) / self.getTableSize()

    def getInequalityFactor(self, fieldname, value, op):
        value = int(value)
        hist = self.histograms[fieldname]
        if hist.checkUniformDist():
            if op == '<':
                return (value - hist.min) / hist.interval
            elif op == '<=':
                return (value - hist.min + 1) / hist.interval
            elif op == '>':
                return (hist.max - value) / hist.interval
            elif op == '>=':
                return (hist.max - value + 1) / hist.interval
            else:
                return -1
        else:
            if op in ['<=', '<']:
                return hist.countSmaller(value) / self.tablesize
            elif op in ['>=', '>']:
                return hist.countLarger(value) / self.tablesize
            else:
                return -1

