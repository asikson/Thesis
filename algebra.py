import row
import db_plugin as dbp
import mystatistics as ms
import struct_plugin as sp
import index as idx
from sql_output import Table
from numpy import prod
from math import ceil
from json import load


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
        self.passBufferMaxSize = getParam("passBufferSize")

    def __iter__(self):
        for buffer in self.data:
            for rec in buffer: 
                self.cost += 1
                newRow = rec.copy().project(self.fields)
                self.passBuffer.append(newRow)
                if len(self.passBuffer) == self.passBufferMaxSize:
                    yield self.passBuffer
                    self.passBuffer = []
        if self.passBuffer != []:
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
        self.passBufferMaxSize = getParam("passBufferSize")

    def __iter__(self):
        for buffer in self.data:
            for rec in buffer:
                self.cost += 1
                if rec.select(self.predicates):
                    self.passBuffer.append(rec)
                    if len(self.passBuffer) == self.passBufferMaxSize:
                        yield self.passBuffer
                        self.passBuffer = []
        if self.passBuffer != []:
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
        self.kind = getParam("joinKind")

        self.estRedFactor = self.estimateRedFactor()
        self.estSize = self.estimateSize()

        self.fieldsLeft, self.fieldsRight = self.preparePredicates()
        self.rightIndex = idx.checkIfIndexExists(
            self.right.tablename, 
            list(map(
                lambda f: f.name,
                self.fieldsRight)))

        self.cost = 0
        self.costCumulative = 0

        self.hashBuffer = dict()
        self.hashBufferMaxSize = getParam("hashBufferSize")
        self.passBuffer = []
        self.passBufferMaxSize = getParam("passBufferSize")

        self.estCost = self.estimateCost()
        self.estCostCumulative = self.estimateCostCumulative()


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
                            if len(self.passBuffer) == self.passBufferMaxSize:
                                yield self.passBuffer
                                self.passBuffer = []
            if self.passBuffer != []:
                yield self.passBuffer
        else:
            if self.kind == 'index' \
                or self.rightIndex:
                it = self.indexJoin()
            elif self.kind == 'hash':
                it = self.hashJoin()
            else:
                assert(False), 'Unknown join kind'

            for rec in it:
                self.passBuffer.append(rec)
                if len(self.passBuffer) == self.passBufferMaxSize:
                    yield self.passBuffer
                    self.passBuffer = []
            if self.passBuffer != []:
                yield self.passBuffer
        self.sumUpCost()
        print(self.costInfo())

    def hashJoin(self):
        for lb in self.left:
            for l in lb:
                self.cost += 1
                k = l.valuesForFields(self.fieldsLeft)
                if k in self.hashBuffer.keys():
                    self.hashBuffer[k].append(l)
                else:
                    self.hashBuffer[k] = [l]
                if len(self.hashBuffer) == self.hashBufferMaxSize:
                    for rec in self.iterRight():
                        yield rec
                    self.hashBuffer = dict()
        if len(self.hashBuffer) != 0:
            for rec in self.iterRight():
                yield rec

    def iterRight(self):
        for rb in self.right:
            for r in rb:
                self.cost += 1
                k = r.valuesForFields(self.fieldsRight)
                if k in self.hashBuffer.keys():
                    for v in self.hashBuffer[k]:
                        newRow = v.copy().concat(r)
                        yield newRow

    def indexJoin(self):
        names2Index = list(map(lambda f: f.name, self.fieldsRight))
        if not self.rightIndex:
            index = idx.Index(self.right.tablename, names2Index)
            self.rightIndex = index.filename
            self.cost += self.right.estSize
        else:
            print('Found index {0}!'.format(self.rightIndex))

        self.predicates += self.right.predicates
        self.right = ReadPkDict(Table(self.right.tablename, None))
        idxPlugin = dbp.DbPlugin(self.rightIndex)
        idxPlugin.open()

        for lb in self.left:
            for l in lb:
                self.cost += 1
                k = l.valuesForFields(self.fieldsLeft)
                pks = idxPlugin.getValuesByIndexKey(k)
                if pks != -1:
                    for pk in pks:
                        r = self.right.get(pk)
                        if r != -1:
                            newRow = l.copy().concat(r)
                            if newRow.select(self.predicates):
                                yield newRow
        idxPlugin.close()

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
        if self.withDict or self.rightIndex:
            return self.left.estSize
        else:
            if self.kind == "hash":
                return self.left.estSize \
                    + ceil(self.left.estSize / self.hashBufferMaxSize) \
                        * self.right.estSize
            elif self.kind == 'index':
                return self.right.estSize + self.left.estSize

    def estimateCostCumulative(self):
        if self.withDict or self.rightIndex or self.kind == 'index':
            return self.left.estCostCumulative + self.estCost
        else:
            if self.kind == 'hash':
                return self.left.estCostCumulative \
                    + ceil(self.left.estSize / self.hashBufferMaxSize) \
                        * self.right.estCostCumulative \
                    + self.estCost

    def costInfo(self):
        return 'Cost of {0} joining {1}: {2} (est. cost: {3} with est. red. {4})'.format(
            self.nameOfJoin(),
            self.right.tablename,
            '{:.3f}'.format(self.cost),
            '{:.3f}'.format(self.estCost),
            '{:.3f}'.format(self.estRedFactor))

    def nameOfJoin(self):
        if self.rightIndex:
            return '(by index)'
        elif self.withDict:
            return ''
        else:
            return self.kind

    def sumUpCost(self):
        self.costCumulative += self.left.costCumulative \
            + self.right.costCumulative + self.cost


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

    def preparePredicates(self):
        for p in self.predicates:
            p.orderRightByTable(self.right.tablename)
        toSplit = list(filter(
            lambda p: not p.withValue,
            self.predicates))

        fieldsRight = list(map(
            lambda p: p.right, toSplit))
        fieldsLeft = list(map(
            lambda p: p.left, toSplit))

        self.predicates = list(filter(
            lambda p: p.withValue,
            self.predicates))

        return fieldsLeft, fieldsRight


class CrossProduct:
    def __init__(self, left, right):
        self.left = left
        self.right = right

        self.cost = 0
        self.costCumulative = 0

        self.estSize = self.left.estSize * self.right.estSize
        self.estCost = self.left.estSize * self.right.estSize
        self.estCostCumulative = left.estCostCumulative \
            + self.left.estSize * self.right.estCostCumulative \
            + self.estCost

        self.passBuffer = []
        self.passBufferMaxSize = getParam("passBufferSize")

    def __iter__(self):
        for lb in self.left:
            for rb in self.right:
                for l in lb:
                    for r in rb:
                        self.cost += 1
                        newRow = l.copy().concat(r)
                        self.passBuffer.append(newRow)
                        if len(self.passBuffer) == self.passBufferMaxSize:
                            yield self.passBuffer
                            self.passBuffer = []
        if self.passBuffer != []:
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


    def __str__(self):
        return '{0}\ncross {1}'.format(
            self.left.__str__(),
            self.right.__str__())


class ReadWithSelection:
    def __init__(self, table, predicates):
        self.tablename = table.name
        self.plugin = dbp.DbPlugin(self.tablename)
        self.plugin.open()
        self.predicates = predicates

        self.cost = 0
        self.costCumulative = 0

        self.estRedFactor = self.estimateRedFactor()
        self.estCost = ms.getTablesize(self.tablename)
        self.estSize = self.estRedFactor * self.estCost
        self.estCostCumulative = self.estCost

        self.readBuffer = []
        self.readBufferMaxSize = getParam("readBufferSize")
        self.passBuffer = []
        self.passBufferMaxSize = getParam("passBufferSize")

    def __iter__(self):
        for rb in self.iterReadBuffer():
            for r in rb:
                self.passBuffer.append(r)
                if len(self.passBuffer) == self.passBufferMaxSize:
                    yield self.passBuffer
                    self.passBuffer = []
        if self.passBuffer != []:
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

            if len(self.readBuffer) == self.readBufferMaxSize:
                yield self.readBuffer
                self.readBuffer = []

        if self.readBuffer != []:
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
        self.plugin.open()

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


def getParam(paramName):
    with open('params/engine_params.txt', 'r') as f:
        params = load(f)
    if paramName in params.keys():
        return params[paramName]
    else:
        assert(False), "Parameter not found"