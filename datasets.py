from database import Record

class Dataset:
    def __init__(self, *args):
        # from table
        if len(args) == 2:
            table, alias = args[0], args[1]
            self.size = len(table.data)
            self.rows = [Row(alias, rec) 
                for pk, rec in table.data.items()]
        # empty
        else:
            self.size = 0
            self.rows = []

    def addRow(self, row):
        self.rows.append(row)
        self.size += 1

    def getRow(self, idx):
        return self.rows[idx]

    def __str__(self):
        return '\n'.join(map(str, self.rows))
    
    def execute(self):
        return self

class Row:
    def __init__(self, *args):
        # copy other row
        if len(args) == 1:
            values = args[0]
            self.values = values
        # from record
        elif len(args) == 2:
            self.values = dict()
            tablename, record = args[0], args[1]
            for c, v in record.items():
                self.values[(tablename, c)] = v 
        # empty
        else:
            self.values = dict()
    
    def __str__(self):
        return ' '.join(map(str, self.values.items()))

    def concat(self, other):
        for k, v in other.values.items():
            self.values[k] = v
        return self

    def copy(self):
        return Row(self.values.copy())
    
    def project(self, fields):
        fields = [(f.tablename, f.name) for f in fields]
        filtered = dict()
        for v in self.values.items():
            if v[0] in fields:
                filtered[v[0]] = v[1]
        self.values = filtered

    def select(self, predicates):
        for p in predicates:
            if not self.checkPredicate(p):
                return False

        return True

    def checkPredicate(self, predicate):
        leftVal = self.valueForField(predicate.left)
        if predicate.withValue:
            return leftVal == predicate.right
        else:
            rightVal = self.valueForField(predicate.right)
    
            return leftVal == rightVal

    def valueForField(self, field):
        field = (field.tablename, field.name)
        if field in self.values.keys():
            return self.values[field]
        else:
            return -1

class Operator:
    def __init__(self, dataset):
        self.dataset = dataset
        self.idx = -1
        self.counter = 0 

    def next(self):
        if self.end():
            return -1
        else:
            self.idx += 1
            self.counter += 1
            return self.dataset.getRow(self.idx)

    def reset(self):
        self.idx = -1
    
    def end(self):
        return self.idx == self.dataset.size - 1
    
    def cost(self):
        return self.counter
