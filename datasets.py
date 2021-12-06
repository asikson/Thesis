class Row:
    def __init__(self):
        self.values = dict()

    @staticmethod
    def rowFromRecord(record, tablename):
        newRow = Row()
        for c, v in record.items():
            newRow.values[(tablename, c)] = v

        return newRow

    def copy(self):
        newRow = Row()
        newRow.values = self.values.copy()

        return newRow

    def __str__(self):
        return ' '.join(map(str, self.values.items()))

    def concat(self, other):
        for k, v in other.values.items():
            self.values[k] = v
        return self
    
    def project(self, fields):
        fields = [(f.tablename, f.name) for f in fields]
        filtered = dict()
        for k, v in self.values.items():
            if k in fields:
                filtered[k] = v
        self.values = filtered

    def select(self, predicates):
        for p in predicates:
            if not self.checkPredicate(p):
                return False

        return True

    def checkPredicate(self, predicate):
        leftVal = self.valueForField(predicate.left)
        if predicate.withValue:
            rightVal = predicate.right
        else:
            rightVal = self.valueForField(predicate.right)
    
        if predicate.operator == '=':
            return leftVal == rightVal
        elif predicate.operator == '!=':
            return leftVal != rightVal
        elif predicate.operator == '>':
            return leftVal > rightVal
        elif predicate.operator == '<':
            return leftVal < rightVal
        else:
            return -1

    def valueForField(self, field):
        field = (field.tablename, field.name)
        if field in self.values.keys():
            return self.values[field]
        else:
            print('No such field')
            return -1

class Dataset:
    def __init__(self):
        self.size = 0
        self.rows = []

    def fillRows(self, rows):
        self.size = len(rows)
        self.rows = rows

    def fillFromTable(self, table, alias):
        self.size = len(table.data)
        self.rows = [Row.rowFromRecord(rec, alias)
            for pk, rec in table.data.items()]

    def addRow(self, row):
        self.rows.append(row)
        self.size += 1

    def getRowFromIdx(self, idx):
        return self.rows[idx]

    def __str__(self):
        return '\n'.join(map(str, self.rows))

    def execute(self):
        return self

class DatasetPkDict:
    def __init__(self, table, alias):
        self.pk_name = table.pk_name
        self.rows = dict()
        for pk, rec in table.data.items():
            self.rows[pk] = Row(alias, rec)

    def getRowFromPk(self, pk):
        return self.rows[pk]

    def __str__(self):
        return '\n'.join(map(lambda r: '* ' + self.pk_name + ': ' + \
            r.__str__(), self.rows))

    def execute(self):
        dataset = Dataset()
        dataset.fillRows(list(self.rows.values()))

        return dataset

class Operator:
    def __init__(self, dataset):
        self.dataset = dataset
        self.idx = 0
        self.counter = 0 

    def current(self):
        return self.dataset.getRowFromIdx(self.idx)

    def next(self):
        if self.end():
            print('Operator out of range')
            return -1
        else:
            self.idx += 1
            self.counter += 1

    def reset(self):
        self.idx = 0
    
    def end(self):
        return self.idx == self.dataset.size - 1

    def projectCurrent(self, fields):
        self.current.project(fields)

    def selectCurrent(self, predicates):
        self.current.select(predicates)

    def cost(self):
        return self.counter