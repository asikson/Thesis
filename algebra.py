# klasy do algebry relacji

class Projection:
    def __init__(self, fields, selection):
        self.fields = fields
        self.wildcard = (len(fields) == 0)
        self.selection = selection

    def __str__(self):
        result = "PROJECT: "
        if self.wildcard:
            result += "[*] "
        else: 
            result += str(self.fields)

        return result + "\n" + self.selection.__str__()

class Selection:
    def __init__(self, predicates, data):
        self.predicates = predicates
        self.data = data

    def __str__(self):
        return "SELECT {" +\
            ", ".join(map(lambda p: p.__str__(), self.predicates)) +\
            "} \n" + self.data.__str__()

class CrossProduct:
    def __init__(self, tables):
        self.tables = tables
    
    def __str__(self):
        return "CROSS:" + str(self.tables)

class Rename:
    def __init__(self, alias, table):
        self.alias = alias
        self.table = table

    def __str__(self):
        return "RENAME " + self.alias + " [" + self.table + "]"

class Predicate:
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def __str__(self):
        return str(self.left) + " = " + str(self.right)

class Join:
    def __init__(self, table1, table2, field1, field2):
        self.table1 = table1
        self.table2 = table2
        self.field1 = field1
        self.field2 = field2
