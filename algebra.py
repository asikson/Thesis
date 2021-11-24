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

class Selection:
    def __init__(self, predicates, dataset):
        self.predicates = predicates
        self.dataset = dataset

    def __str__(self):
        return "SELECT: " +\
            ", ".join(map(str, self.predicates)) +\
            "\n" + self.dataset.__str__()

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

class Read:
    def __init__(self, dataset):
        self.dataset = dataset