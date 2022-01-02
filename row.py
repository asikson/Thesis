import functools as ft

class Row:
    def __init__(self):
        self.values = dict()

    def __str__(self):
        return ' '.join(map(str, self.values.items()))

    @staticmethod
    def rowFromRecord(tablename, columns, values):
        newRow = Row()
        for c, v in zip(columns, values):
            newRow.values[(tablename, c)] = v

        return newRow

    def copy(self):
        newRow = Row()
        newRow.values = self.values.copy()

        return newRow

    def concat(self, other):
        for k, v in other.values.items():
            self.values[k] = v
        return self
    
    def project(self, fields):
        def fieldSort(p1, p2):
            return fields.index(p1[0]) < fields.index(p2[0])

        fields = [(f.tablename, f.name) for f in fields]
        filtered = dict()

        for k, v in self.values.items():
            if k in fields:
                filtered[k] = v
        self.values = {k: v for k, v in sorted(filtered.items(), key=ft.cmp_to_key(fieldSort))}
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

        if not isinstance(rightVal, str):
            leftVal = int(leftVal)
    
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
            print(field)
            print(self.values.keys())
            assert(False)
            print('No such field')
            return -1