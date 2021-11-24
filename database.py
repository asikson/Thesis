import datasets as ds

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

class myDatabase:
    def __init__(self):
        self.tables = dict()
        self.tableNames = []

    def addTable(self, tablename, pk, columns):
        self.tables[tablename] = Table(tablename, pk, columns)
        self.tableNames.append(tablename)
    
    def printTable(self, tablename):
        print(self.tables[tablename])

    def addRecord(self, tablename, record):
        if tablename not in self.tableNames:
            print("No such table")
        else:
            self.tables[tablename].addRecord(record)

    def getTable(self, tablename):
        return self.tables[tablename]


db = myDatabase()

# PEOPLE
db.addTable("people", "id", ["name", "surname", "city"])

db.addRecord("people", Record(1, ["Joanna", "Mielniczuk", 1]))
db.addRecord("people", Record(2, ["Marianna", "Kabata", 2]))
db.addRecord("people", Record(3, ["Olga", "Sokołowska", 2]))

# CITIES
db.addTable("cities", "id", ["name"])

db.addRecord("cities", Record(1, ["Wrocław"]))
db.addRecord("cities", Record(2, ["Warszawa"]))

print(ds.Dataset(db.getTable('people')))