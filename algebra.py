import datasets as ds

class Projection:
    def __init__(self, fields, dataset):
        self.fields = fields
        self.wildcard = (len(fields) == 0)
        self.dataset = dataset
        self.op = ds.Operator(self.dataset)

    def __str__(self):
        result = "PROJECTION: "
        if self.wildcard:
            result += "* "
        else: 
            result += ", ".join(map(str, self.fields))
        result += "\n" + self.dataset.__str__()

        return result

    def execute(self):
        self.executeDataset()
        if not self.wildcard:
            while not self.op.end():
                self.op.projectCurrent(self.fields)
                self.op.next()
            self.printCost()

        return self.dataset

    def executeDataset(self):
        self.dataset = self.dataset.execute()
        self.op = ds.Operator(self.dataset)

    def cost(self):
        return self.op.cost()

    def printCost(self):
        print('Cost of PROJECTION: ', self.cost())

class Selection:
    def __init__(self, predicates, dataset):
        self.predicates = predicates
        self.dataset = dataset
        self.op = ds.Operator(self.dataset)

    def __str__(self):
        return "SELECTION: " + \
            ", ".join(map(str, self.predicates)) + \
            "\n" + self.dataset.__str__()

    def execute(self):
        self.executeDataset()
        output = ds.Dataset()

        while not self.op.end():
            if self.op.current().select(self.predicates):
                output.addRow(self.op.current())
            self.op.next()
        self.dataset = output

        self.printCost()
        return self.dataset

    def executeDataset(self):
        self.dataset = self.dataset.execute()
        self.op = ds.Operator(self.dataset)

    def cost(self):
        return self.op.cost()

    def printCost(self):
        print('Cost of SELECTION: ', self.cost())

class Join:
    def __init__(self, dataset_1, dataset_2, field_1, field_2):
        self.dataset_1 = dataset_1
        self.dataset_2 = dataset_2
        self.field_1 = field_1
        self.field_2 = field_2

    def execute(self):
        self.executeDatasets()

    def executeDatasets(self):
        self.dataset_1 = self.dataset_1.execute()
        self.dataset_2 = self.dataset_2.execute()
        self.op1 = ds.Operator(self.dataset_1)
        self.op2 = ds.Operator(self.dataset_2)

class CrossProduct:
    def __init__(self, dataset_1, dataset_2):
        self.dataset_1 = dataset_1
        self.dataset_2 = dataset_2
        self.op1 = ds.Operator(self.dataset_1)
        self.op2 = ds.Operator(self.dataset_2)

    def execute(self):
        self.executeDatasets()
        output = ds.Dataset()

        while not self.op1.end():
            left = self.op1.current()
            while not self.op2.end():
                right = self.op2.current()
                row = left.copy().concat(right)
                output.addRow(row)
                self.op2.next()
            self.op2.reset()
            self.op1.next()
   
        self.printCost()
        return output

    def executeDatasets(self):
        self.dataset_1 = self.dataset_1.execute()
        self.dataset_2 = self.dataset_2.execute()
        self.op1 = ds.Operator(self.dataset_1)
        self.op2 = ds.Operator(self.dataset_2)

    def cost(self):
        return self.op1.cost() + self.op2.cost()

    def printCost(self):
        print('Cost of CROSS JOIN: ', self.cost())

class Read:
    def __init__(self, table, database):
        self.tablename = table.name
        self.alias = table.alias
        self.database = database

    def execute(self):
        output = ds.Dataset()
        output.fillFromTable(self.database.getTable(self.tablename), self.alias)

        return output