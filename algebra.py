import row
import berkeley as brk
import info

class Projection:
    def __init__(self, fields, data):
        self.fields = fields
        self.wildcard = (len(fields) == 0)
        self.data = data
        self.cost = 0

    def __iter__(self):
        for rec in self.data:
            self.cost += 1
            yield rec.copy().project(self.fields)
        print('Projection cost: ' + str(self.cost))
        self.cost += self.data.cost

class Selection:
    def __init__(self, predicates, data):
        self.predicates = predicates
        self.data = data
        self.cost = 0

    def __iter__(self):
        for rec in self.data:
            self.cost += 1
            if rec.select(self.predicates):
                yield rec
        print('Selection cost: ' + str(self.cost))
        self.cost += self.data.cost

class Join:
    def __init__(self, left, right, leftField, rightField):
        self.left = left
        self.right = right
        self.leftField = leftField
        self.rightField = rightField
        self.cost = 0

    def __iter__(self):
        if not isinstance(self.right, ReadPkDict):
            for l in self.left:
                k = l.valueForField(self.leftField)
                for r in self.right:
                    self.cost += 1
                    if r.valueForField(self.rightField) == k:
                        yield l.copy().concat(r)
        else:
            for l in self.left:
                self.cost += 1
                k = l.valueForField(self.leftField)
                r = self.right.get(k)
                if r != 1:
                    yield l.copy().concat(r)
        print('Join cost: ' + str(self.cost))
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
        print('Cross cost: ' + str(self.cost))
        self.cost += self.left.cost + self.right.cost

class Read:
    def __init__(self, outputTable):
        self.tablename = outputTable.name
        self.alias = outputTable.alias
        self.cost = 0

    def __iter__(self):
        for rec in brk.tableIterator(self.tablename):
            self.cost += 1
            yield row.Row.rowFromRecord(self.alias, 
                info.getTableColumns(self.tablename), 
                brk.getValuesFromRecord(rec))
        print('Read cost: ' + str(self.cost))

class ReadPkDict:
    def __init__(self, outputTable):
        self.tablename = outputTable.name
        self.alias = outputTable.alias
        self.cost = 0

    def get(self, pk):
        return row.Row.rowFromRecord(self.alias,
            info.getTableColumns(self.tablename),
            brk.getValuesByPk(self.tablename, pk))