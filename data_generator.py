from bsddb3 import db
from random import randrange

numOfEmp = 1000
numOfDep = 10
numOfCities = 50
numOfFirstNames = 300
numOfLastNames = 500
lowestAge = 20
highestAge = 40

# SAMPLES
samplesPath = '/home/asikson/Pulpit/inż/Thesis/sample_data/'

def getNSamplesFromFile(n, filename):
    file = open(samplesPath + filename + '.txt', 'r')
    return [line.strip() for line in file.readlines()[:n]]

def getRandomSample(samples):
    return samples[randrange(len(samples))]

# employees
def generateEmpData():
    filename = 'employees'
    employeesDB = db.DB()
    employeesDB.open(filename, dbtype=db.DB_HASH, flags=db.DB_CREATE)

    f_names = getNSamplesFromFile(numOfFirstNames, 'first_names')
    l_names = getNSamplesFromFile(numOfLastNames, 'last_names')

    for i in range(numOfEmp):
        fn = getRandomSample(f_names)

        values = '{first_name}\0{last_name}\0{age}\0{gender}'.format(
            first_name = fn,
            last_name = getRandomSample(l_names),
            age = randrange(highestAge - lowestAge) + lowestAge,
            gender = 'F' if fn[-1] == 'a' else 'M'
        )

        employeesDB.put(bytes(str(i+1), 'utf-8'), values)

    employeesDB.close()

# cities
def generateCitiesData():
    filename = 'cities'
    citiesDB = db.DB()
    citiesDB.open(filename, dbtype=db.DB_HASH, flags=db.DB_CREATE)

    cities = getNSamplesFromFile(numOfCities, 'cities')
    i = 1
    for c in cities:
        citiesDB.put(bytes(str(i), 'utf-8'), c)
        i += 1
    citiesDB.close()

# departments
def generateDeptData():
    filename = 'departments'
    departmentsDB = db.DB()
    departmentsDB.open(filename, dbtype=db.DB_HASH, flags=db.DB_CREATE)

    dept_names = getNSamplesFromFile(numOfDep, 'plant_names')
    i = 1
    for n in dept_names:
        values = '{dept_name}\0{city_id}'.format(
            dept_name = n,
            city_id = randrange(numOfCities) + 1
        )
        departmentsDB.put(bytes(str(i), 'utf-8'), values)
        i += 1

    departmentsDB.close()

######################################################################

generateDeptData()