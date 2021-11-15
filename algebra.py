# klasy do algebry relacji

class Projection:
    def __init__(self, fields, selection):
        self.fields = fields
        self.wildcard = (len(fields) == 0)
        self.selection = selection

    def toString(self):
        result = "\project: "
        if self.wildcard:
            result += "[*] "
        else: 
            result += str(self.fields)

        return result + "\n" + self.selection.toString()

class Selection:
    def __init__(self, predicates, data):
        self.predicates = predicates
        self.data = data

    def toString(self):
        return "\select {" +\
            ", ".join(map(lambda p: p.toString(), self.predicates)) +\
            "} \n" + self.data.toString()

class CrossProduct:
    def __init__(self, tables, renames):
        self.tables = tables
        self.renames = renames
    
    def toString(self):
        return "(" + " x ".join(self.tables) + ")"

class Rename:
    def __init__(self, alias, table):
        self.alias = alias
        self.table = table

class Predicate:
    def __init__(self, table1, field1, table2, field2, value):
        self.table1 = table1
        self.field1 = field1
        self.table2 = table2
        self.field2 = field2
        self.value = value

    def toString(self):
        result = self.table1 + "." + self.field1 + " = "
        if self.value is None:
            result += self.table2 + "." + self.field2
        else:
            result += self.value
        
        return result
