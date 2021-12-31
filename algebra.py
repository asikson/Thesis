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
        self.estCost = data.estSize
        self.estSize = data.estSize

    def __iter__(self):
        for rec in self.data:
            self.cost += 1
            yield rec.copy().project(self.fields)
        self.sumUpCost() 

    def sumUpCost(self):
        self.cost += self.data.cost

class Selection:
    def __init__(self, predicates, data):
        self.predicates = predicates
        self.data = data
        self.cost = 0
        self.estCost = data.estSize
        self.estSize = prod(list(map(ms.reductionFactor, predicates))) * data.estSize

    def __iter__(self):
        for rec in self.data:
            self.cost += 1
            if rec.select(self.predicates):
                yield rec
        self.sumUpCost()

    def sumUpCost(self):
        self.cost += self.data.cost

class Join:
    def __init__(self, left, right, fieldPairs, fk):
        self.left = left
        self.right = right
        self.fieldPairs = fieldPairs
        # if right is ReadPkDict
        self.fk = fk
        self.cost = 0
        if isinstance(self.right, ReadPkDict):
            self.estCost = self.left.estSize
        else:
            self.estSize = self.left.estSize * self.right.estSize

    def __iter__(self):
        if isinstance(self.right, ReadPkDict):
            for l in self.left:
                self.cost += 1
                k = l.valueForField(self.fk)
                r = self.right.get(k)
                if r != -1:
                    good = True
                    for p in self.fieldPairs:
                        if l.valueForField(p[0]) != r.valueForField(p[1]):
                            good = False
                            break
                    if good:
                        yield l.copy().concat(r)
        else:
            for l in self.left:
                for r in self.right:
                    self.cost += 1
                    good = True
                    for p in self.fieldPairs:
                        if l.valueForField(p[0]) != r.valueForField(p[1]):
                            good = False
                            break
                    if good:
                        yield l.copy().concat(r)
        self.sumUpCost()

    def sumUpCost(self):
        self.cost += self.left.cost + self.right.cost

class CrossProduct:
    def __init__(self, left, right):
        self.left = left
        self.right = right
        self.cost = 0

    def __iter__(self):
        for l in self.left:
            for r in self.right:
                self.cost += 1
                yield l.copy().concat(r)
        self.cost += self.left.cost + self.right.cost

class Read:
    def __init__(self, table):
        self.tablename = table.name
        self.cost = 0

    def __iter__(self):
        columns = info.getTableColumns(self.tablename)
        for rec in brk.tableIterator(self.tablename):
            self.cost += 1
            yield row.Row.rowFromRecord(self.tablename,
                columns, 
                brk.getValuesFromRecord(rec))

class ReadPkDict:
    def __init__(self, table):
        self.tablename = table.name
        self.cost = 0

    def get(self, pk):
        values = brk.getValuesByPk(self.tablename, pk)
        if values != -1:
            return row.Row.rowFromRecord(self.tablename,
                info.getTableColumns(self.tablename),
                values)
        else:
            return -1

class ReadWithSelection:
    def __init__(self, table, predicates):
        self.tablename = table.name
        self.predicates = predicates
        self.cost = 0

    def __iter__(self):
        columns = info.getTableColumns(self.tablename)
        for rec in brk.tableIterator(self.tablename):
            self.cost += 1
            input = row.Row.rowFromRecord(self.tablename,
                columns, 
                brk.getValuesFromRecord(rec))
            if input.select(self.predicates):
                yield 