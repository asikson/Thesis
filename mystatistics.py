import berkeley as brk
import info
from math import inf
from scipy.stats import chisquare

statistics = dict()
numOfBuckets = 20

def getStatistics(tablename):
    if not tablename in statistics.keys():
        createStatistics(tablename)
    return statistics[tablename]

def createStatistics(tablename):
    statistics[tablename] = Statistics(tablename)

def reductionFactor(predicate):
    # predicates with values
    if predicate.withValue:
        tablename, field, op, val = predicate.split()
        stat = getStatistics(tablename)

        # finding specific values
        if op == '=':
            # primary keys
            if info.isTablesPk(tablename, field):
                return 1.0 / stat.tablesize
            # other fields
            else:
                t = info.getFieldType(tablename, field)
                # string fields
                if t == str:
                    return 1.0 / stat.countDiffValues(field)
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
        chisq, p = chisquare(observations)
        return p >= 0.05

    def printBuckets(self):
        for b in self.buckets:
            print(b)

class Statistics:
    def __init__(self, tablename):
        self.tablename = tablename
        self.columns = info.getTableColumns(tablename)
        self.tablesize = self.getTableSize()
        self.histograms = dict()

    def valueForField(self, rec, field):
        idx = self.columns.index(field)
        values = brk.getValuesFromRecord(rec)
        
        return values[idx]

    def getTableSize(self):
        rows = 0
        for _ in brk.tableIterator(self.tablename):
            rows += 1
        
        return rows 

    def countDiffValues(self, fieldname):
        count = 0
        visited = set()

        for rec in brk.tableIterator(self.tablename):
            val = self.valueForField(rec, fieldname)
            if val not in visited:
                visited.add(val)
                count += 1

        return count

    def findMinMax(self, fieldname):
        min, max = inf, -1 * inf
        for rec in brk.tableIterator(self.tablename):
            val = int(self.valueForField(rec, fieldname))
            if val > max:
                max = val
            elif val < min:
                min = val

        return min, max

    def createEquiwidthHistogram(self, fieldname):
        min, max = self.findMinMax(fieldname)
        width = (max - min) / numOfBuckets

        histogram = EquiwidthHistogram(min, max, width)
        for rec in brk.tableIterator(self.tablename):
            val = int(self.valueForField(rec, fieldname))
            histogram.addValue(val)
        
        return histogram

    def getHistogramFactorForField(self, fieldname, value):
        value = int(value)
        if not fieldname in self.histograms.keys():
            self.histograms[fieldname] = self.createEquiwidthHistogram(fieldname)
        histogram = self.histograms[fieldname]
        if histogram.checkUniformDist():
            return 1 / self.countDiffValues(fieldname)
        else:
            return histogram.factorForValue(value) / self.tablesize

    def getInequalityFactor(self, fieldname, value, op):
        min, max = self.findMinMax(fieldname)
        if value >= min and value <= max:
            interval = max - min
            if op == '<':
                return (value - min) / interval
            elif op == '<=':
                return (value - min + 1) / interval
            elif op == '>':
                return (max - value) / interval
            elif op == '>=':
                return (max - value + 1) / interval
            else:
                return -1
        else:
            return 0