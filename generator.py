import db_plugin as dbp
from json import load
from random import randrange, shuffle
from numpy.random import default_rng
from math import sqrt

class Generator:
    def __init__(self):
        with open('params/generator_params.txt') as f:
            self.params = load(f)

        self.generateEmp()
        self.generateCities()
        self.generateDept()
        self.generateDeptEmp()
        self.generateSalaries()
        self.generateDeptManager()

    def getParam(self, name):
        if name in self.params.keys():
            return self.params[name]
        else:
            assert(False), "Parameter not found"

    def randomNumber(self, min, max, distribution):
        if distribution == 'uniform':
            return randrange(max - min) + min
        elif distribution == 'normal':
            rng = default_rng()
            mean = (max - min) / 2 + min
            deviation = sqrt(mean - min)
            return round(rng.normal(loc=mean, scale=deviation))
        else:
            assert(False), 'Distribution not recognized'

    def getSamplesFromFile(self, path):
        with open(path) as f:
            return [line.strip()
                for line in f]

    def drawSample(self, samples, distribution):
        idx = self.randomNumber(0, len(samples), distribution)
        return samples[idx]

    def getGenderSamples(self, femalePercent):
        malePercent = 100 - femalePercent
        female = ['F' for _ in range(femalePercent)]
        male = ['M' for _ in range(malePercent)]
        genders = female + male
        shuffle(genders)

        return genders

    def generateEmp(self):
        plugin = dbp.DbPlugin('employees')
        plugin.createTable(self.empStream())
    
    def empStream(self):
        numOfEmp = self.getParam("numOfEmp")
        firstNamesFile = self.getParam("firstNameSamplesFile")
        firstNameDist = self.getParam("firstNameDist")
        lastNamesFile = self.getParam("lastNameSamplesFile")
        lastNameDist = self.getParam("lastNameDist")
        minAge = self.getParam("minAge")
        maxAge = self.getParam("maxAge")
        ageDist = self.getParam("ageDist")
        femalePercent = self.getParam("femalePercent")

        firstNameSamples = self.getSamplesFromFile(firstNamesFile)
        lastNameSamples = self.getSamplesFromFile(lastNamesFile)
        genderSamples = self.getGenderSamples(femalePercent)

        for id in range(1, numOfEmp + 1):
            firstName = self.drawSample(firstNameSamples, firstNameDist)
            lastName = self.drawSample(lastNameSamples, lastNameDist)
            age = self.randomNumber(minAge, maxAge, ageDist)
            gender = self.drawSample(genderSamples, 'uniform')

            yield id, [firstName, lastName, age, gender]

        print('Generated {0} employees!'.format(numOfEmp))

    def generateCities(self):
        plugin = dbp.DbPlugin('cities')
        plugin.createTable(self.citiesStream())

    def citiesStream(self):
        numOfCities = self.getParam("numOfCities")
        cityNameSamplesFile = self.getParam("cityNameSamplesFile")
        cityNameDist = self.getParam("cityNameDist")
        cityNameSamples = self.getSamplesFromFile(cityNameSamplesFile)

        for id in range(1, numOfCities + 1):
            cityName = self.drawSample(cityNameSamples, cityNameDist)

            yield id, [cityName]
        
        print('Generated {0} cities!'.format(numOfCities))

    def generateDept(self):
        plugin = dbp.DbPlugin('departments')
        plugin.createTable(self.deptStream())

    def deptStream(self):
        numOfDept = self.getParam("numOfDept")
        deptNameSamplesFile = self.getParam("deptNameSamplesFile")
        deptNameDist = self.getParam("deptNameDist")
        deptNameSamples = self.getSamplesFromFile(deptNameSamplesFile)

        for id in range(1, numOfDept + 1):
            deptName = self.drawSample(deptNameSamples, deptNameDist)
            city = self.randomNumber(1, self.getParam("numOfCities") + 1, 'uniform')

            yield id, [deptName, city]

        print('Generated {0} departments!'.format(numOfDept))

    def generateDeptEmp(self):
        plugin = dbp.DbPlugin('dept_emp')  
        plugin.createTable(self.deptEmpStream())

    def deptEmpStream(self):
        deptEmpDist = self.getParam("deptEmpDist")
        empIndexes = list(range(1, self.getParam("numOfEmp") + 1))
        shuffle(empIndexes)

        for i, empIdx in enumerate(empIndexes):
            deptIdx = self.randomNumber(1, self.getParam("numOfDept") + 1, deptEmpDist)
            yield i + 1, [deptIdx, empIdx]

        print('Generated dept_emp data!')

    def generateSalaries(self):
        plugin = dbp.DbPlugin('salaries')
        plugin.createTable(self.salariesStream())

    def salariesStream(self):
        minSalary = self.getParam("minSalary")
        maxSalary = self.getParam("maxSalary")
        salaryDist = self.getParam("salaryDist")
        empIndexes = list(range(1, self.getParam("numOfEmp") + 1))
        shuffle(empIndexes)

        for i, empIdx in enumerate(empIndexes):
            salary = self.randomNumber(minSalary, maxSalary, salaryDist)
            yield i, [salary, empIdx]

        print('Generated salaries!')

    def generateDeptManager(self):
        plugin = dbp.DbPlugin('dept_manager')
        plugin.createTable(self.deptManagerStream())

    def deptManagerStream(self):
        deptIndexes = list(range(1, self.getParam("numOfDept") + 1))
        shuffle(deptIndexes)

        for i, deptIdx in enumerate(deptIndexes):
            empIdx = self.randomNumber(1, self.getParam("numOfEmp") + 1, 'uniform')
            yield i + 1, [deptIdx, empIdx]

        print('Generated dept_manager data!')

generator = Generator()