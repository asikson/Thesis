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
        self.estCostCumulative = data.estCostCumulative + self.estCost
        self.estSize = data.estSize

    def __iter__(self):
        for rec in self.data:
            self.cost += 1
            yield rec.copy().project(self.fields)
        self.sumUpCost() 

    def sumUpCost(self):
        self.cost += self.data.cost

    def __str__(self):
        return (
            'project (' + str(self.estCost) + ', ' + str(self.data.estSize) + ') ' + \
            ', '.join(list(map(str, self.fields))) + \
            '\n' + self.data.__str__())

class Selection:
    def __init__(self, predicates, data):
        self.predicates = predicates
        self.data = data
        self.cost = 0
        self.estCost = data.estSize
        self.estCostCumulative = data.estCostCumulative + self.estCost
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
        return (
            'select (' + str(self.estCost) + ', ' + str(self.data.estSize) + ') ' + \
            ', '.join(list(map(str, self.predicates))) + '\n' + \
            self.data.__str__())

class Join:
    def __init__(self, left, right, predicates, fk):
        self.left = left
        self.right = right
        self.predicates = predicates
        # if right is ReadPkDict
        self.fk = fk
        self.cost = 0
        self.predReduction = 1.0 if self.predicates == [] \
                else prod(list(map(ms.reductionFactor, predicates)))
        if isinstance(self.right, ReadPkDict):
            self.estCost = self.left.estSize
            self.estSize = self.predReduction * self.left.estSize
        else:
            self.estCost = self.left.estSize * self.right.estSize
            self.estSize = self.predReduction * self.left.estSize * self.right.estSize
        self.estCostCumulative = left.estCostCumulative + right.estCostCumulative + self.estCost

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
        costs = [str(self.estCost), str(self.left.estSize), str(self.right.estSize)]
        result = self.left.__str__() + \
            '\njoin (' + ', '.join(costs) + ') ' + \
            self.right.__str__() + \
            ' on ' + ', '.join(list(map(str, self.predicates))) + \
            ' {' + str(self.predReduction) + '}'
        if self.fk is not None:
            result += ' (by fk: ' + self.fk.__str__() + ')'
        
        return result

class CrossProduct:
    def __init__(self, left, right):
        self.left = left
        self.right = right
        self.cost = 0
        self.estCost = self.left.estSize * self.right.estSize
        self.estSize = self.left.estSize * self.right.estSize
        self.estCostCumulative = left.estCostCumulative + right.estCostCumulative + self.estCost

    def __iter__(self):
        for l in self.left:
            for r in self.right:
                self.cost += 1
                yield l.copy().concat(r)
        self.sumUpCost()

    def sumUpCost(self):
        self.cost += self.left.cost + self.right.cost

    def __str__(self):
        costs = [str(self.estCost), str(self.left.estSize), str(self.right.estSize)]
        return self.left.__str__() + \
            '\ncross (' + ', '.join(costs) + ') ' + \
            self.right.__str__()

class Read:
    def __init__(self, table):
        self.tablename = table.name
        self.cost = 0
        self.estCost = ms.getStatistics(self.tablename).tablesize
        self.estSize = self.estCost
        self.estCostCumulative = self.estCost

    def __iter__(self):
        columns = info.getTableColumns(self.tablename)
        for rec in brk.tableIterator(self.tablename):
            self.cost += 1
            yield row.Row.rowFromRecord(self.tablename,
                columns, 
                brk.getValuesFromRecord(rec))

    def __str__(self):
        return 'READ (' + self.tablename + ') ' + \
            '[' + str(self.estCost) + ']'

class ReadPkDict:
    def __init__(self, table):
        self.tablename = table.name
        self.cost = 0
        self.estCost = 0
        self.estSize = 0
        self.estCostCumulative = 0

    def get(self, pk):
        values = brk.getValuesByPk(self.tablename, pk)
        if values != -1:
            return row.Row.rowFromRecord(self.tablename,
                info.getTableColumns(self.tablename),
                values)
        else:
            return -1

    def __str__(self):
        return 'READ dict (' + self.tablename + ')'

class ReadWithSelection:
    def __init__(self, table, predicates):
        self.tablename = table.name
        self.predicates = predicates
        self.cost = 0
        self.estCost = ms.getStatistics(self.tablename).tablesize
        self.reductionFactor = prod(list(map(ms.reductionFactor, predicates)))
        self.estSize = self.reductionFactor * self.estCost
        self.estCostCumulative = self.estCost

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
        return'READ with sel. (' + self.tablename + ') ' + \
            '[' + str(self.estCost) + ']' + \
            '\nwith ' + ', '.join(list(map(str, self.predicates)))