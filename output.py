class Predicate:
    def __init__(self, left, right):
        self.left = left
        self.right = right
        self.withValue = not isinstance(right, Field)

    def __str__(self):
        result = self.left.__str__() + " = "
        if self.withValue:
            result += "'" + self.right + "'"
        else:
            result += self.right.__str__()

        return result

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
