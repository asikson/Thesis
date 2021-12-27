from bsddb3 import db
from random import randrange, shuffle

numOfEmp = 1000
numOfDept = 50
numOfCities = 100
numOfFirstNames = 300
numOfLastNames = 500
minAge = 20
maxAge = 40
minSalary = 3500
maxSalary = 10000

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
    empDB = db.DB()
    empDB.open(filename, dbtype=db.DB_HASH, flags=db.DB_CREATE)

    f_names = getNSamplesFromFile(numOfFirstNames, 'first_names')
    l_names = getNSamplesFromFile(numOfLastNames, 'last_names')

    for i in range(numOfEmp):
        fn = getRandomSample(f_names)

        values = '{first_name}\0{last_name}\0{age}\0{gender}'.format(
            first_name = fn,
            last_name = getRandomSample(l_names),
            age = randrange(maxAge - minAge) + minAge,
            gender = 'F' if fn[-1] == 'a' else 'M'
        )

        empDB.put(bytes(str(i+1), 'utf-8'), values)

    empDB.close()

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
    deptDB = db.DB()
    deptDB.open(filename, dbtype=db.DB_HASH, flags=db.DB_CREATE)

    dept_names = getNSamplesFromFile(numOfDept, 'plant_names')
    i = 1
    for n in dept_names:
        values = '{dept_name}\0{city_id}'.format(
            dept_name = n,
            city_id = randrange(numOfCities) + 1
        )
        deptDB.put(bytes(str(i), 'utf-8'), values)
        i += 1

    deptDB.close()

# dept - emp
def generateDeptEmpData():
    filename = 'dept_emp'
    deptEmpDB = db.DB()
    deptEmpDB.open(filename, dbtype=db.DB_HASH, flags=db.DB_CREATE)

    i = 0
    for emp in range(numOfEmp):
        values = '{dept_id}\0{emp_id}'.format(
            dept_id = randrange(numOfDept) + 1,
            emp_id = emp + 1)
        deptEmpDB.put(bytes(str(i), 'utf-8'), values)
        i += 1

    deptEmpDB.close()

# salaries
def generateSalariesData():  
    filename = 'salaries'
    salariesDB = db.DB()
    salariesDB.open(filename, dbtype=db.DB_HASH, flags=db.DB_CREATE)

    i = 0
    for emp in range(numOfEmp):
        values = '{salary}\0{emp_id}'.format(
            salary = randrange(maxSalary - minSalary) + minSalary,
            emp_id = emp + 1)
        salariesDB.put(bytes(str(i), 'utf-8'), values)
        i += 1

    salariesDB.close()

# dept - manager
def generateDeptManagerData():
    filename = 'dept_manager'
    deptManagerDB = db.DB()
    deptManagerDB.open(filename, dbtype=db.DB_HASH, flags=db.DB_CREATE)

    emp = [*range(numOfEmp)]
    shuffle(emp)
    emp = emp[:numOfDept]

    i = 0
    for d in range(numOfDept):
        values = '{dept_id}\0{emp_id}'.format(
            dept_id = d + 1,
            emp_id = emp[i] + 1
        )

        i += 1
        deptManagerDB.put(bytes(str(i), 'utf-8'), values)

    deptManagerDB.close()

######################################################################

generateEmpData()
generateCitiesData()
generateDeptData()
generateDeptEmpData()
generateDeptManagerData()
generateSalariesData()