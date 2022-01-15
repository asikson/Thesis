import row
import db_plugin as dbp
import mystatistics as ms
import struct_plugin as sp
from numpy import prod
from math import ceil

passBufferSize = 10000
readBufferSize = 2000000
hashBufferSize = 1000000


class Projection:
    def __init__(self, fields, data):
        self.fields = fields
        self.data = data

        self.cost = 0
        self.costCumulative = 0

        self.estSize = data.estSize
        self.estCost = data.estSize
        self.estCostCumulative = data.estCostCumulative + self.estCost

        self.passBuffer = []

    def __iter__(self):
        for buffer in self.data:
            for rec in buffer: 
                self.cost += 1
                newRow = rec.copy().project(self.fields)
                self.passBuffer.append(newRow)
                if self.isPassBufferFull():
                    yield self.passBuffer
                    self.passBuffer = []
        if not self.isPassBufferEmpty():
            yield self.passBuffer
        self.sumUpCost() 
        print(self.costInfo())


    # costs
    def costInfo(self):
        return 'Cost of projecting {0}: {1} (est. cost: {2})'.format(
            self.fieldsToStr(),
            '{:.3f}'.format(self.cost),
            '{:.3f}'.format(self.estCost))

    def sumUpCost(self):
        self.costCumulative += self.data.costCumulative + self.cost


    # buffer
    def isPassBufferFull(self):
        return len(self.passBuffer) == passBufferSize
    
    def isPassBufferEmpty(self):
        return self.passBuffer == []


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

        self.estRedFactor = prod(list(map(ms.reductionFactor, predicates)))
        self.estSize = self.estRedFactor * data.estSize
        self.estCost = data.estSize
        self.estCostCumulative = data.estCostCumulative + self.estCost

        self.passBuffer = []

    def __iter__(self):
        for buffer in self.data:
            for rec in buffer:
                self.cost += 1
                if rec.select(self.predicates):
                    self.passBuffer.append(rec)
                    if self.isPassBufferFull():
                        yield self.passBuffer
                        self.passBuffer = []
        if not self.isPassBufferEmpty():
            yield self.passBuffer
        self.sumUpCost()
        print(self.costInfo())


    # costs
    def costInfo(self):
        return 'Cost of selecting {0}: {1} (est. cost: {2} with est. red. {3})'.format(
            self.predsToStr(),
            '{:.3f}'.format(self.cost),
            '{:.3f}'.format(self.estCost),
            '{:.3f}'.format(self.estRedFactor))

    def sumUpCost(self):
        self.costCumulative += self.data.costCumulative + self.cost


    # buffer
    def isPassBufferFull(self):
        return len(self.passBuffer) == passBufferSize
    
    def isPassBufferEmpty(self):
        return self.passBuffer == []


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

        self.estRedFactor = self.estimateRedFactor()
        self.estSize = self.estimateSize()
        self.estCost = self.estimateCost()
        self.estCostCumulative = self.left.estCostCumulative + \
            ceil(self.left.estSize / hashBufferSize) * self.right.estCostCumulative + \
            self.estCost

        self.passBuffer = []
        self.hashBuffer = dict()

    def __iter__(self):
        if self.withDict:
            for lb in self.left:
                for l in lb:
                    self.cost += 1
                    k = l.valueForField(self.fk)
                    r = self.right.get(k)
                    if r != -1:
                        newRow = l.copy().concat(r)
                        if newRow.select(self.predicates):
                            self.passBuffer.append(newRow)
                            if self.isPassBufferFull():
                                yield self.passBuffer
                                self.passBuffer = []
            if not self.isPassBufferEmpty():
                yield self.passBuffer
        # hash join
        else:
            for rec in self.hashJoin():
                self.passBuffer.append(rec)
                if self.isPassBufferFull():
                    yield self.passBuffer
                    self.passBuffer = []
            if not self.isPassBufferEmpty():
                yield self.passBuffer
        self.sumUpCost()
        print(self.costInfo())

    def hashJoin(self):
        for p in self.predicates:
            p.orderRightByTable(self.right.tablename)
        fieldsLeft = list(map(
            lambda p: p.left, self.predicates))

        for lb in self.left:
            for l in lb:
                self.cost += 1
                k = l.valuesForFields(fieldsLeft)
                if k in self.hashBuffer.keys():
                    self.hashBuffer[k].append(l)
                else:
                    self.hashBuffer[k] = [l]
                if self.isHashBufferFull():
                    for rec in self.iterRight():
                        yield rec
                    self.hashBuffer = dict()
        if not self.isHashBufferEmpty():
            for rec in self.iterRight():
                yield rec

    def iterRight(self):
        fieldsRight = list(map(
            lambda p: p.right, self.predicates))

        for rb in self.right:
            for r in rb:
                self.cost += 1
                k = r.valuesForFields(fieldsRight)
                if k in self.hashBuffer.keys():
                    for v in self.hashBuffer[k]:
                        newRow = v.copy().concat(r)
                        yield newRow


    # costs
    def estimateRedFactor(self):
        if self.predicates == []:
            return 1.0
        else:
            return prod(list(map(ms.reductionFactor, self.predicates)))

    def estimateSize(self):
        if self.withDict:
            return self.estRedFactor * self.left.estSize
        else:
            return self.estRedFactor * self.left.estSize * self.right.estSize

    def estimateCost(self):
        if self.withDict:
            return self.left.estSize
        else:
            return self.left.estSize + \
                ceil(self.left.estSize / hashBufferSize) * self.right.estSize

    def costInfo(self):
        return 'Cost of joining {0}: {1} (est. cost: {2} with est. red. {3})'.format(
            self.right.tablename,
            '{:.3f}'.format(self.cost),
            '{:.3f}'.format(self.estCost),
            '{:.3f}'.format(self.estRedFactor))

    def sumUpCost(self):
        self.costCumulative += self.left.costCumulative + \
            self.right.costCumulative + self.cost


    # buffers
    def isPassBufferFull(self):
        return len(self.passBuffer) == passBufferSize
    
    def isPassBufferEmpty(self):
        return self.passBuffer == []

    def isHashBufferFull(self):
        return len(self.hashBuffer) == hashBufferSize
    
    def isHashBufferEmpty(self):
        return len(self.hashBuffer) == 0


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

        self.estSize = self.left.estSize * self.right.estSize
        self.estCost = self.left.estSize * self.right.estSize
        self.estCostCumulative = left.estCostCumulative + \
            self.left.estSize * self.right.estCostCumulative + self.estCost

        self.passBuffer = []

    def __iter__(self):
        for lb in self.left:
            for rb in self.right:
                for l in lb:
                    for r in rb:
                        self.cost += 1
                        newRow = l.copy().concat(r)
                        self.passBuffer.append(newRow)
                        if self.isPassBufferFull():
                            yield self.passBuffer
                            self.passBuffer = []
        if not self.isPassBufferEmpty():
            yield self.passBuffer
        self.sumUpCost()
        print(self.costInfo())


    #costs
    def costInfo(self):
        return 'Cost of crossing: {0} (est. cost: {1})'.format(
            '{:.3f}'.format(self.cost),
            '{:.3f}'.format(self.estCost))

    def sumUpCost(self):
        self.costCumulative += self.left.costCumulative + \
            self.right.costCumulative + self.cost


    # buffer
    def isPassBufferFull(self):
        return len(self.passBuffer) == passBufferSize
    
    def isPassBufferEmpty(self):
        return self.passBuffer == []


    def __str__(self):
        return '{0}\ncross {1}'.format(
            self.left.__str__(),
            self.right.__str__())


class ReadWithSelection:
    def __init__(self, table, predicates):
        self.tablename = table.name
        self.plugin = dbp.DbPlugin(self.tablename)
        self.predicates = predicates

        self.cost = 0
        self.costCumulative = 0

        self.estRedFactor = self.estimateRedFactor()
        self.estCost = ms.getTablesize(self.tablename)
        self.estSize = self.estRedFactor * self.estCost
        self.estCostCumulative = self.estCost

        self.readBuffer = []
        self.passBuffer = []

    def __iter__(self):
        for rb in self.iterReadBuffer():
            for r in rb:
                self.passBuffer.append(r)
                if self.isPassBufferFull():
                    yield self.passBuffer
                    self.passBuffer = []
        if not self.isPassBufferEmpty():
            yield self.passBuffer

    def iterReadBuffer(self):
        columns = sp.getTableColumns(self.tablename)

        for rec in self.plugin.tableIterator():
            self.cost += 1
            newRow = row.Row.rowFromRecord(self.tablename,
                columns, 
                self.plugin.decodePair(rec))

            if (self.predicates == []
                or newRow.select(self.predicates)):
                self.readBuffer.append(newRow)

            if self.isReadBufferFull():
                yield self.readBuffer
                self.readBuffer = []

        if not self.isReadBufferEmpty():
            yield self.readBuffer
            self.readBuffer = []

        self.sumUpCost()
        print(self.costInfo())


    # costs
    def estimateRedFactor(self):
        if self.predicates == []:
            return 1.0
        else:
            return prod(list(map(ms.reductionFactor, self.predicates)))

    def costInfo(self):
        return 'Cost of reading {0} with {1}: {2} (est. cost: {3} with est. red. {4})'.format(
            self.tablename,
            self.predsToStr(),
            '{:.3f}'.format(self.cost),
            '{:.3f}'.format(self.estCost),
            '{:.3f}'.format(self.estRedFactor))

    def sumUpCost(self):
        self.costCumulative += self.cost


    # buffers
    def isReadBufferFull(self):
        return len(self.readBuffer) == readBufferSize
    
    def isReadBufferEmpty(self):
        return self.readBuffer == []

    def isPassBufferFull(self):
        return len(self.passBuffer) == passBufferSize
    
    def isPassBufferEmpty(self):
        return self.passBuffer == []


    def __str__(self):
        return 'READ with sel. ({0}) on [{1}]'.format(
            self.tablename,
            self.predsToStr())
    
    def predsToStr(self):
        return ', '.join(list(map(str, self.predicates)))


class ReadPkDict:
    def __init__(self, table):
        self.tablename = table.name
        self.plugin = dbp.DbPlugin(self.tablename)

        self.cost = 0
        self.costCumulative = 0

        self.estSize = 0
        self.estCost = 0
        self.estCostCumulative = self.estCost

    def get(self, pk):
        values = self.plugin.getValuesByKey(pk)

        if values != -1:
            newRow = row.Row.rowFromRecord(self.tablename,
                sp.getTableColumns(self.tablename),
                values)
            return newRow

    def __str__(self):
        return 'READ DICT ({0})'.format(self.tablename)
