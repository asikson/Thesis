import datasets as ds

class Projection:
    def __init__(self, fields, dataset):
        self.fields = fields
        self.wildcard = (len(fields) == 0)
        self.dataset = dataset

    def __str__(self):
        result = "PROJECT: "
        if self.wildcard:
            result += "* "
        else: 
            result += ", ".join(map(str, self.fields))

        return result + "\n" + self.dataset.__str__()

    def execute(self):
        self.dataset = self.dataset.execute()
        if self.wildcard:
            return self.dataset
        op = ds.Operator(self.dataset)

        while not op.end():
            op.next().project(self.fields)
        
        print('Projection: ' + str(op.cost()))
        return self.dataset

class Selection:
    def __init__(self, predicates, dataset):
        self.predicates = predicates
        self.dataset = dataset

    def __str__(self):
        return "SELECT: " +\
            ", ".join(map(str, self.predicates)) +\
            "\n" + self.dataset.__str__()

    def execute(self):
        self.dataset = self.dataset.execute()
        op = ds.Operator(self.dataset)
        output = ds.Dataset()

        while not op.end():
            row = op.next()
            if row.select(self.predicates):
                output.addRow(row)
        self.dataset = output

        print('Selection: ' + str(op.cost()))
        return self.dataset

class Join:
    def __init__(self, dataset_1, dataset_2, field_1, field_2):
        self.dataset_1 = dataset_1
        self.dataset_2 = dataset_2
        self.field_1 = field_1
        self.field_2 = field_2

class CrossProduct:
    def __init__(self, dataset_1, dataset_2):
        self.dataset_1 = dataset_1
        self.dataset_2 = dataset_2

    def execute(self):
        op1 = ds.Operator(self.dataset_1.execute())
        op2 = ds.Operator(self.dataset_2.execute())
        output = ds.Dataset()

        while not op1.end():
            left = op1.next()
            while not op2.end():
                right = op2.next()
                row = left.copy().concat(right)
                output.addRow(row)
            op2.reset()
        
        cost = op1.cost() + op2.cost()
        print('Cross: ' + str(cost))
        return output

class Read:
    def __init__(self, dataset):
        self.dataset = dataset

    def execute(self):
        return self.dataset