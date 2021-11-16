tables = dict()

class Table:
    def __init__(self, name, pk, columns):
        self.name = name
        self.pk = pk
        self.columns = columns
        self.data = dict()

    def addRecord(self, record):
        if record.pk in self.data.keys():
            print("Primary key violation")
        else:
            self.data[record.pk] = dict()
            for c, v in zip(self.columns, record.values):
                self.data[record.pk][c] = v
            
    def __str__(self):
        return self.name.upper() + "\n" + self.pk \
            + " * " + " * ".join(self.columns) \
            + "\n" + "\n".join(map(lambda r: str(r[0]) + " " \
            + " ".join(map(str, r[1].values())), self.data.items()))

class Record:
    def __init__(self, pk, values):
        self.pk = pk
        self.values = values

    def __str__(self):
        return self.pk + " -> ".join(self.values)

def addTable(tablename, pk, columns):
    global tables
    tables[tablename] = Table(tablename, pk, columns)

def printTable(tablename):
    print(tables[tablename])

def addRecord(tablename, record):
    if tablename not in tables.keys():
        print("No such table")
    else:
        tables[tablename].addRecord(record)


# PEOPLE
addTable("people", "id", ["name", "surname", "city"])

addRecord("people", Record(1, ["Joanna", "Mielniczuk", 1]))
addRecord("people", Record(2, ["Marianna", "Kabata", 2]))
addRecord("people", Record(3, ["Olga", "Sokołowska", 2]))

# CITIES
addTable("cities", "id", ["name"])

addRecord("cities", Record(1, ["Wrocław"]))
addRecord("cities", Record(2, ["Warszawa"]))


printTable("people")
printTable("cities")
