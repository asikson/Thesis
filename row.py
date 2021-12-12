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
        return self

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