class SqlOutput:
    def __init__(self, fields, predicates, tables):
        self.fields = fields
        self.predicates = predicates
        self.tables = tables

    def __str__(self):
        return "Fields: " + ', '.join(map(str, self.fields)) + "\n" \
            + "Predicates: " + ', '.join(map(str, self.predicates)) + "\n" \
            + "Tables: " + ', '.join(map(str, self.tables))
    
    def get(self):
        return self.fields, self.predicates, self.tables

class Field:
    def __init__(self, tablename, name):
        self.tablename = tablename
        self.name = name

    def __str__(self):
        return self.tablename + "." + self.name

class Predicate:
    def __init__(self, left, right, operator):
        self.left = left
        self.right = right
        self.operator = operator
        self.withValue = not isinstance(right, Field)

    def __str__(self):
        result = self.left.__str__() + ' ' + self.operator
        if self.withValue:
            result += ' ' + str(self.right)
        else:
            result += ' ' + self.right.__str__()

        return result

    def split(self):
        if self.withValue:
            return self.left.tablename, self.left.name, \
                self.operator, self.right
        else:
            return self.left.tablename, self.left.name, \
                self.operator, self.right.tablename, \
                self.right.name

    def orderRightByTable(self, tablename):
        if self.withValue:
            return -1
        elif self.right.tablename != tablename:
            temp = self.left
            self.left = self.right
            self.right = temp

class Table:
    def __init__(self, name, alias):
        self.name = name
        self.alias = alias
    
    def __str__(self):
        return self.name + " (" + self.alias + ")"


def formatField(pair):
    return Field(pair[0], pair[1])

def formatFields(fields):
    return list(map(formatField, fields))

def formatTables(tables):
    return list(map(
        lambda t: Table(t[0], t[1]) if len(t) > 1
            else Table(t[0], t[0]), tables))

def formatPredicate(left, right, op):
    return Predicate(left, right, op)

def formatOutput(fields, predicates, tables):
    return SqlOutput(fields, predicates, tables)