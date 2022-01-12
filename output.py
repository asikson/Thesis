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

class Field:
    def __init__(self, name, tablename):
        self.name = name
        self.tablename = tablename

    def __str__(self):
        return self.tablename + "." + self.name

class evaluatorOutput:
    def __init__(self, fields, predicates, tables):
        self.fields = fields
        self.predicates = predicates
        self.tables = tables

    def __str__(self):
        return "FIELDS: " + ', '.join(map(str, self.fields)) + "\n" \
            + "PREDICATES: " + ', '.join(map(str, self.predicates)) + "\n" \
            + "TABLES: " + ', '.join(map(str, self.tables))
    
    def get(self):
        return self.fields, self.predicates, self.tables
