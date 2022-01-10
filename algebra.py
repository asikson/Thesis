import row
import berkeley as brk
import info
import mystatistics as ms
from numpy import prod

bufferSize = 100

class Projection:
    def __init__(self, fields, data):
        self.fields = fields
        self.data = data
        self.cost = 0
        self.costCumulative = 0
        self.estCost = data.estSize
        self.estCostCumulative = data.estCostCumulative + self.estCost
        self.estSize = data.estSize

    def __iter__(self):
        rowBuffer = []
        for buffer in self.data:
            for rec in buffer: 
                self.cost += 1
                rowBuffer.append(rec.copy().project(self.fields))
                if len(rowBuffer) == bufferSize:
                    yield rowBuffer
                    rowBuffer = []
        if len(rowBuffer) == bufferSize:
            yield rowBuffer
        self.sumUpCost() 
        print(self.costInfo())

    def costInfo(self):
        return 'Cost of projecting {0}: {1} (est. cost: {2})'.format(
            self.fieldsToStr(),
            '{:.3f}'.format(self.cost),
            '{:.3f}'.format(self.estCost))

    def sumUpCost(self):
        self.costCumulative += self.data.costCumulative + self.cost

    def __str__(self):
        return 'Project ({0}) \n{1}'.format(
            self.fieldsToStr(),
            self.data.__str__())

    def fieldsToStr(self):
        return ', '.join(list(map(str, self.fields)))


class Selection:
    def __init__(self, predicates, data):
        self.predicates = predicates
        self.data = data
        self.cost = 0
        self.costCumulative = 0
        self.estCost = data.estSize
        self.estCostCumulative = data.estCostCumulative + self.estCost
        self.estRedFactor = prod(list(map(ms.reductionFactor, predicates)))
        self.estSize = self.estRedFactor * data.estSize

    def __iter__(self):
        rowBuffer = []
        for buffer in self.data:
            for rec in buffer:
                self.cost += 1
                if rec.select(self.predicates):
                    rowBuffer.append(rec)
                    if len(rowBuffer) == bufferSize:
                        yield rowBuffer
                        rowBuffer = []
        if rowBuffer != []:
            yield rowBuffer
        self.sumUpCost()
        print(self.costInfo())

    def costInfo(self):
        return 'Cost of selecting {0}: {1} (est. cost: {2} with est. red. {3})'.format(
            self.predsToStr(),
            '{:.3f}'.format(self.cost),
            '{:.3f}'.format(self.estCost),
            '{:.3f}'.format(self.estRedFactor))

    def sumUpCost(self):
        self.costCumulative += self.data.costCumulative + self.cost

    def __str__(self):
        return 'select ({0}) from\n{1}'.format(
            self.predsToStr(),
            self.data.__str__())

    def predsToStr(self):
        return ', '.join(list(map(str, self.predicates)))


class Join:
    def __init__(self, left, right, predicates, fk):
        self.left = left
        self.right = right
        self.predicates = predicates
        self.withDict = isinstance(self.right, ReadPkDict)
        self.fk = fk if self.withDict else None
        self.cost = 0
        self.costCumulative = 0
        self.estCost = self.estimateCost()
        self.estCostCumulative = self.left.estCostCumulative + \
            self.right.estCostCumulative + self.estCost
        self.estRedFactor = self.estimateRedFactor()
        self.estSize = self.estimateSize()

    def estimateRedFactor(self):
        if self.predicates == []:
            return 1.0
        else:
            return prod(list(map(ms.reductionFactor, self.predicates)))

    def estimateCost(self):
        if self.withDict:
            return self.left.estSize
        else:
            return self.left.estSize * self.right.estSize

    def estimateSize(self):
        if self.withDict:
            return self.estRedFactor * self.left.estSize
        else:
            return self.estRedFactor * self.left.estSize * self.right.estSize

    def __iter__(self):
        rowBuffer = []
        if self.withDict:
            for lb in self.left:
                for l in lb:
                    self.cost += 1
                    k = l.valueForField(self.fk)
                    r = self.right.get(k)
                    if r != -1:
                        rec = l.copy().concat(r)
                        if rec.select(self.predicates):
                            rowBuffer.append(rec)
                            if len(rowBuffer) == bufferSize:
                                yield rowBuffer
                                rowBuffer = []
            if rowBuffer != []:
                yield rowBuffer

        else:
            for lb in self.left:
                for rb in self.right:
                    for l in lb:
                        for r in rb:
                            self.cost += 1
                            rec = l.copy().concat(r)
                            if rec.select(self.predicates):
                                rowBuffer.append(rec)
                                if len(rowBuffer) == bufferSize:
                                    yield rowBuffer
                                    rowBuffer = []
            if rowBuffer != []:
                yield rowBuffer
        self.sumUpCost()
        print(self.costInfo())

    def costInfo(self):
        return 'Cost of joining {0}: {1} (est. cost: {2} with est. red. {3})'.format(
            self.right.tablename,
            '{:.3f}'.format(self.cost),
            '{:.3f}'.format(self.estCost),
            '{:.3f}'.format(self.estRedFactor))

    def sumUpCost(self):
        self.costCumulative += self.left.costCumulative + \
            self.right.costCumulative + self.cost

    def __str__(self):
        result = '{0}\n join {1} on {2}'.format(
            self.left.__str__(),
            self.right.__str__(),
            self.predsToStr())
        if self.fk is not None:
            result += ' (by fk: {0})'.format(self.fk.__str__())
        
        return result

    def predsToStr(self):
        return ', '.join(list(map(str, self.predicates)))


class CrossProduct:
    def __init__(self, left, right):
        self.left = left
        self.right = right
        self.cost = 0
        self.costCumulative = 0
        self.estCost = self.left.estSize * self.right.estSize
        self.estSize = self.left.estSize * self.right.estSize
        self.estCostCumulative = left.estCostCumulative + \
            right.estCostCumulative + self.estCost

    def __iter__(self):
        rowBuffer = []
        for lb in self.left:
            for rb in self.right:
                for l in lb:
                    for r in rb:
                        self.cost += 1
                        rowBuffer.append(l.copy().concat(r))
                        if len(rowBuffer) == bufferSize:
                            yield rowBuffer
                            rowBuffer = []
        if rowBuffer != []:
            yield rowBuffer
        self.sumUpCost()
        print(self.costInfo())

    def costInfo(self):
        return 'Cost of crossing: {0} (est. cost: {1})'.format(
            '{:.3f}'.format(self.cost),
            '{:.3f}'.format(self.estCost))

    def sumUpCost(self):
        self.costCumulative += self.left.costCumulative + \
            self.right.costCumulative + self.cost

    def __str__(self):
        return '{0}\ncross {1}'.format(
            self.left.__str__(),
            self.right.__str__())


class Read:
    def __init__(self, table):
        self.tablename = table.name
        self.cost = 0
        self.costCumulative = 0
        self.estCost = ms.getStatistics(self.tablename).tablesize
        self.estCostCumulative = self.estCost
        self.estSize = self.estCost
        self.buffer = []

    def __iter__(self):
        if self.buffer is not None: 
            if self.buffer == []:
                self.fillBuffer()
            rowBuffer = []
            for rec in self.buffer:
                rowBuffer.append(rec)
                if len(rowBuffer) == bufferSize:
                    yield rowBuffer
                    rowBuffer = []
            if rowBuffer != []:
                yield rowBuffer

    def fillBuffer(self):
        columns = info.getTableColumns(self.tablename)
        for rec in brk.tableIterator(self.tablename):
            self.cost += 1
            input = row.Row.rowFromRecord(self.tablename,
                columns, 
                brk.getValuesFromRecord(rec))
            self.buffer.append(input)
        if self.buffer == []:
            self.buffer = None
        self.sumUpCost()
        print(self.costInfo())

    def costInfo(self):
        return 'Cost of reading {0}: {1} (est. cost: {2})'.format(
            self.tablename,
            '{:.3f}'.format(self.cost),
            '{:.3f}'.format(self.estCost))

    def __str__(self):
        return 'READ ({0})'.format(self.tablename)
    
    def sumUpCost(self):
        self.costCumulative += self.cost


class ReadWithSelection:
    def __init__(self, table, predicates):
        self.tablename = table.name
        self.predicates = predicates
        self.buffer = []
        self.cost = 0
        self.costCumulative = 0
        self.estCost = ms.getStatistics(self.tablename).tablesize
        self.estCostCumulative = self.estCost
        self.estRedFactor = self.estimateRedFactor()
        self.estSize = self.estRedFactor * self.estCost

    def estimateRedFactor(self):
        return prod(list(map(ms.reductionFactor, self.predicates)))

    def __iter__(self):
        if self.buffer is not None: 
            if self.buffer == []:
                self.fillBuffer()
            rowBuffer = []
            for rec in self.buffer:
                rowBuffer.append(rec)
                if len(rowBuffer) == bufferSize:
                    yield rowBuffer
                    rowBuffer = []
            if rowBuffer != []:
                yield rowBuffer

    def fillBuffer(self):
        columns = info.getTableColumns(self.tablename)
        for rec in brk.tableIterator(self.tablename):
            self.cost += 1
            input = row.Row.rowFromRecord(self.tablename,
                columns, 
                brk.getValuesFromRecord(rec))
            if input.select(self.predicates):
                self.buffer.append(input)
        if self.buffer == []:
            self.buffer = None
        self.sumUpCost()
        print(self.costInfo())

    def costInfo(self):
        return 'Cost of reading {0} with {1}: {2} (est. cost: {3} with est. red. {4})'.format(
            self.tablename,
            self.predsToStr(),
            '{:.3f}'.format(self.cost),
            '{:.3f}'.format(self.estCost),
            '{:.3f}'.format(self.estRedFactor))

    def __str__(self):
        return'READ with sel. (' + self.tablename + ') ' + \
            '[' + str(self.estCost) + ']' + \
            '\nwith ' + ', '.join(list(map(str, self.predicates)))

    def sumUpCost(self):
        self.costCumulative += self.cost
    
    def predsToStr(self):
        return ', '.join(list(map(str, self.predicates)))


class ReadPkDict:
    def __init__(self, table):
        self.tablename = table.name
        self.cost = 0
        self.costCumulative = 0
        self.estCost = 0
        self.estCostCumulative = 0
        self.estSize = 0
        self.buffer = dict()

    def get(self, pk):
        if pk in self.buffer.keys():
            val = self.buffer[pk]
            if val is None:
                return -1
            else:
                return val
        else:
            self.addToBuffer(pk)
            return self.get(pk)
    
    def addToBuffer(self, pk):
        values = brk.getValuesByPk(self.tablename, pk)
        if values != -1:
            input = row.Row.rowFromRecord(self.tablename,
                info.getTableColumns(self.tablename),
                values)
            self.buffer[pk] = input
        else:
            self.buffer[pk] = None

    def __str__(self):
        return 'READ DICT ({0})'.format(self.tablename)
