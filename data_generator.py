from bsddb3 import db
from random import randrange

# SAMPLES
samplesPath = '/home/asikson/Pulpit/inż/Thesis/sample_data/'

def getSamplesFromFile(filename):
    file = open(samplesPath + filename + '.txt', 'r')
    return [line.strip() for line in file.readlines()]

def getRandomSample(samples):
    return samples[randrange(len(samples))]


# STUDENTS: *id* name surname gender age [city] 
def generateStudentsData(numberOfStudents, numberOfCities):
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
    return i - 1


######################################################################

numberOfStudents = 100
numberOfCities = generateCitiesData()
generateStudentsData(numberOfStudents, numberOfCities)