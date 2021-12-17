from bsddb3 import db
from random import randrange

numberOfCities = 3
numberOfStudents = 10
numberOfTeachers = 3

# SAMPLES
samplesPath = '/home/asikson/Pulpit/inż/Thesis/sample_data/'

def getSamplesFromFile(filename):
    file = open(samplesPath + filename + '.txt', 'r')
    return [line.strip() for line in file.readlines()]

def getRandomSample(samples):
    return samples[randrange(len(samples))]


# STUDENTS: *id* name surname gender age [city] 
def generateStudentsData():
    global numberOfCities, numberOfStudents
    filename = 'students'
    studentsDB = db.DB()
    studentsDB.open(filename, dbtype=db.DB_HASH, flags=db.DB_CREATE)

    names = getSamplesFromFile('names')
    surnames = getSamplesFromFile('surnames')

    for i in range(numberOfStudents):
        n = getRandomSample(names)

        values = '{name}\0{surname}\0{gender}\0{age}\0{city}'.format(
            name = n,
            surname = getRandomSample(surnames),
            gender = 'F' if n[-1] == 'a' else 'M',
            age = randrange(10) + 21,
            city = randrange(numberOfCities) + 1
        )

        studentsDB.put(bytes(str(i+1), 'utf-8'), values)

    studentsDB.close()

# CITIES *id* name 
def generateCitiesData():
    filename = 'cities'
    citiesDB = db.DB()
    citiesDB.open(filename, dbtype=db.DB_HASH, flags=db.DB_CREATE)

    cities = getSamplesFromFile('cities')
    i = 1
    for c in cities:
        citiesDB.put(bytes(str(i), 'utf-8'), c)
        i += 1
    citiesDB.close()

# TEACHERS *id* name surname [city]
def generateTeachersData():
    global numberOfCities
    filename = 'teachers'
    teachersDB = db.DB()
    teachersDB.open(filename, dbtype=db.DB_HASH, flags=db.DB_CREATE)

    names = getSamplesFromFile('names')
    surnames = getSamplesFromFile('surnames')

    for i in range(numberOfTeachers):
        n = getRandomSample(names)

        values = '{name}\0{surname}\0{city}'.format(
            name = n,
            surname = getRandomSample(surnames),
            city = randrange(numberOfCities) + 1
        )

        teachersDB.put(bytes(str(i+1), 'utf-8'), values)

    teachersDB.close()


######################################################################

generateCitiesData()
generateStudentsData()
generateTeachersData()