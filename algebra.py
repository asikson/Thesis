import row
import berkeley as brk
import info
import mystatistics as ms
from numpy import prod

class Projection:
    def __init__(self, fields, data):
        self.fields = fields
        self.data = data
        self.cost = 0
        self.estCost = data.estCost + data.estSize
        self.estSize = data.estSize

    def __iter__(self):
        for rec in self.data:
            self.cost += 1
            yield rec.copy().project(self.fields)
        self.sumUpCost() 

    def sumUpCost(self):
        self.cost += self.data.cost

    def __str__(self):
        return ' -> ' + str(self.estCost) + ' ' + \
            'PROJECT: ' + ', '.join(list(map(str, self.fields))) + \
            ' on\n(' + self.data.__str__() + ')'

class Selection:
    def __init__(self, predicates, data):
        self.predicates = predicates
        self.data = data
        self.cost = 0
        self.estCost = data.estCost + data.estSize
        self.estSize = prod(list(map(ms.reductionFactor, predicates))) * data.estSize

    def __iter__(self):
        for rec in self.data:
            self.cost += 1
            if rec.select(self.predicates):
                yield rec
        self.sumUpCost()

    def sumUpCost(self):
        self.cost += self.data.cost

    def __str__(self):
        return ' -> ' + str(self.estCost) + ' ' + \
            'SELECT: ' + ', '.join(list(map(str, self.predicates))) + \
            ' from\n(' + self.data.__str__() + ')'

class Join:
    def __init__(self, left, right, predicates, fk):
        self.left = left
        self.right = right
        self.predicates = predicates
        # if right is ReadPkDict
        self.fk = fk
        self.cost = 0
        if isinstance(self.right, ReadPkDict):
            self.estCost = self.left.estCost + self.right.estCost + self.left.estSize
            r = 1.0 if self.predicates == [] \
                else prod(list(map(ms.reductionFactor, predicates)))
            self.estSize = r * self.left.estSize
        else:
            self.estCost = self.left.estCost + self.right.estCost + \
                (self.left.estSize * self.right.estSize)
            self.estSize = self.right.reductionFactor * 0.1 * self.estCost

    def __iter__(self):
        if isinstance(self.right, ReadPkDict):
            for l in self.left:
                self.cost += 1
                k = l.valueForField(self.fk)
                r = self.right.get(k)
                if r != -1:
                    rec = l.copy().concat(r)
                    if rec.select(self.predicates):
                        yield rec
        else:
            for l in self.left:
                for r in self.right:
                    self.cost += 1
                    rec = l.copy().concat(r)
                    if rec.select(self.predicates):
                        yield rec
        self.sumUpCost()

    def sumUpCost(self):
        self.cost += self.left.cost + self.right.cost

    def __str__(self):
        result = ' -> ' + str(self.estCost) + ' ' + \
            '(' + self.left.__str__() + ')\nJOIN ' + self.right.__str__() + \
            ' ON ' + ', '.join(list(map(str, self.predicates)))
        if self.fk is not None:
            result += ' (by fk: ' + self.fk.__str__() + ')'
        
        return result

class CrossProduct:
    def __init__(self, left, right):
        self.left = left
        self.right = right
        self.cost = 0
        self.estCost = self.left.estCost + self.right.estCost + \
            (self.left.estSize * self.right.estSize)
        self.estSize = self.estCost

    def __iter__(self):
        for l in self.left:
            for r in self.right:
                self.cost += 1
                yield l.copy().concat(r)
        self.sumUpCost()

    def sumUpCost(self):
        self.cost += self.left.cost + self.right.cost

    def __str__(self):
        return ' -> ' + str(self.estCost) + ' ' + \
            self.left.__str__() + '\nX ' + self.right.__str__()

class Read:
    def __init__(self, table):
        self.tablename = table.name
        self.cost = 0
        self.estCost = ms.getStatistics(self.tablename).tablesize
        self.estSize = self.estCost
        self.reductionFactor = 1.0

    def __iter__(self):
        columns = info.getTableColumns(self.tablename)
        for rec in brk.tableIterator(self.tablename):
            self.cost += 1
            yield row.Row.rowFromRecord(self.tablename,
                columns, 
                brk.getValuesFromRecord(rec))

    def __str__(self):
        return ' -> ' + str(self.estCost) + ' ' + \
            'Read (' + self.tablename + ')'

class ReadPkDict:
    def __init__(self, table):
        self.tablename = table.name
        self.cost = 0
        self.estCost = 0
        self.estSize = 0

    def get(self, pk):
        values = brk.getValuesByPk(self.tablename, pk)
        if values != -1:
            return row.Row.rowFromRecord(self.tablename,
                info.getTableColumns(self.tablename),
                values)
        else:
            return -1

    def __str__(self):
        return ' -> ' + str(self.estCost) + ' ' + \
            'Read dict (' + self.tablename + ')'

class ReadWithSelection:
    def __init__(self, table, predicates):
        self.tablename = table.name
        self.predicates = predicates
        self.cost = 0
        self.estCost = ms.getStatistics(self.tablename).tablesize
        self.reductionFactor = prod(list(map(ms.reductionFactor, predicates)))
        self.estSize = self.reductionFactor * self.estCost

    def __iter__(self):
        columns = info.getTableColumns(self.tablename)
        for rec in brk.tableIterator(self.tablename):
            self.cost += 1
            input = row.Row.rowFromRecord(self.tablename,
                columns, 
                brk.getValuesFromRecord(rec))
            if input.select(self.predicates):
                yield input

    def __str__(self):
        return ' -> ' + str(self.estCost) + ' ' + \
            'Read (' + self.tablename + ')' + \
            ' with ' + ', '.join(list(map(str, self.predicates)))