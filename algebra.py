# klasy do algebry relacji

class Projection:
    def __init__(self, fields, selection):
        self.fields = fields
        self.wildcard = (len(fields) == 0)
        self.selection = selection

    def __str__(self):
        result = "PROJECT: "
        if self.wildcard:
            result += "* "
        else: 
            result += ", ".join(map(str, self.fields))

        return result + "\n" + self.selection.__str__()

class Selection:
    def __init__(self, predicates, data):
        self.predicates = predicates
        self.data = data

    def __str__(self):
        return "SELECT {" +\
            ", ".join(map(lambda p: p.__str__(), self.predicates)) +\
            "} \n" + self.data.__str__()

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

class CrossProductList:
    def __init__(self, tables):
        self.tables = tables
    
    def __str__(self):
        return "CROSS LIST: " + " x ".join(map(str, self.tables))

class Join:
    def __init__(self, table_1, table_2, fieldName_1, fieldName_2):
        self.table_1 = table_1
        self.table_2 = table_2
        self.fieldName_1 = fieldName_1
        self.fieldName_2 = fieldName_2

    def __str__(self):
        return self.table_1.name +  " JOIN " +\
            self.table_2.name + " ON " +\
            self.table_1.alias + "." + self.fieldName_1 + " = " +\
            self.table_2.alias + "." + self.fieldName_2

class CrossProduct:
    def __init__(self, table_1, table_2):
        self.table_1 = table_1
        self.table_2 = table_2

    def __str__(self):
        return "CROSS: " + self.table_1.name +\
            " X " + self.table_2.name

class Table:
    def __init__(self, name, alias):
        self.name = name
        self.alias = alias
    
    def __str__(self):
        return "TABLE: " + self.name + " (" + self.alias + ")"

class Field:
    def __init__(self, name, tablename):
        self.name = name
        self.tablename = tablename

    def __str__(self):
        return "FIELD: " + self.tablename + "." + self.name