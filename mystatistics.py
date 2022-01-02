from sqlparse.tokens import Other, String
import berkeley as brk
import info
from math import inf
import output

numOfBuckets = 5
statistics = dict()

class Bucket:
    def __init__(self, left, right):
        self.left = left
        self.right = right
        self.count = 0

    def bump(self):
        self.count += 1

class EquiwidthHistogram:
    def __init__(self, min, max, width):
        self.min = min
        self.max = max
        self.width = width
        self.buckets = self.createBuckets()

    def createBuckets(self):
        buckets = []
        l = self.min - 0.5 
        r = self.min + self.width + 0.5

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
        for b in self.buckets:
            if value < b.right:
                return b.count * (1.0 / self.width)

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
        return self.histograms[fieldname].factorForValue(value) / self.tablesize

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


def createStatistics(tablename):
    statistics[tablename] = Statistics(tablename)

def getStatistics(tablename):
    if not tablename in statistics.keys():
        createStatistics(tablename)
    return statistics[tablename]

def reductionFactor(predicate):
        operator = predicate.operator

        if predicate.withValue:
            tablename = predicate.left.tablename
            field = predicate.left.name
            value = predicate.right
            stat = getStatistics(tablename)

            if operator == '=':
                if info.isTablesPk(tablename, field):
                    return 1.0 / stat.tablesize
                else:
                    t = info.getFieldType(tablename, field)
                    if t == str:
                        return 1.0 / stat.countDiffValues(field)
                    elif t == int:
                        value = int(value)
                        return stat.getHistogramFactorForField(field, value)
                    else:
                        return - 1
            else:
                return stat.getInequalityFactor(field, value, operator)
        else:
            tablename1 = predicate.left.tablename
            field1 = predicate.left.name
            tablename2 = predicate.right.tablename
            field2 = predicate.right.name

            if info.isForeinKey(tablename1, field1, tablename2, field2):
                stat = getStatistics(tablename2)
                return 1.0 / stat.tablesize
            elif info.isForeinKey(tablename2, field2, tablename1, field1):
                stat = getStatistics(tablename1)
                return 1.0 / stat.tablesize
            else:
                return 0.1




