from random import randrange

class myDatabase:
    def __init__(self):
        self.tables = dict()

    def addTable(self, tablename, pk, columns):
        self.tables[tablename] = Table(tablename, pk, columns)
    
    def printTable(self, tablename):
        print(self.tables[tablename])

    def addRecord(self, tablename, record):
        if tablename not in self.tables.keys():
            print("No such table")
        else:
            self.tables[tablename].addRecord(record)

    def getTable(self, tablename):
        return self.tables[tablename]

class Table:
    def __init__(self, name, pk_name, columns):
        self.name = name
        self.pk_name = pk_name
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
        return self.name.upper() + "\n" + self.pk_name \
            + " * " + " * ".join(self.columns) \
            + "\n" + "\n".join(map(lambda r: str(r[0]) + " " \
            + " ".join(map(str, r[1].values())), self.data.items()))

class Record:
    def __init__(self, pk, values):
        self.pk = pk
        self.values = values

    def __str__(self):
        return self.pk + " -> ".join(self.values)

def createDatabase():
    db = myDatabase()

    names = ['Joanna', 'Marianna', 'Olga', 'Alicja', 
        'Przemysław', 'Artur', 'Antoni', 'Mateusz']
    surnames = ['Nowak', 'Storczyk', 'Kabata', 'Pisarczuk',
        'Kowalczyk', 'Wlazły', 'Maj', 'Kołodziejczyk']
    cities = ['Wrocław', 'Warszawa', 'Kraków', 'Zakopane']

    # CITIES
    db.addTable("cities", "id", ["id", "name"])
    i = 1
    for c in cities:
        db.addRecord("cities", Record(i, [i, c]))
        i += 1

    # PEOPLE
    numberOfPeople = 3
    db.addTable("people", "id", ["id", "name", "surname", "city", "age"])
    for i in range(numberOfPeople):
        n = names[randrange(len(names))]
        s = surnames[randrange(len(surnames))]
        c = randrange(len(cities)) + 1
        a = randrange(10) + 20

        db.addRecord("people", Record(i + 1, [i + 1, n, s, c, a]))

    return db