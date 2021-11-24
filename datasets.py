class Dataset:
    def __init__(self, table, alias):
        self.size = len(table.data)
        self.rows = [Row(alias, rec) 
            for pk, rec in table.data.items()]
    '''
    def __init__(self):
        self.size = 0
        self.rows = []
    '''

    def addRow(self, row):
        self.rows.append(row)
        self.size += 1

    def getRow(self, idx):
        self.rows[idx]

    def __str__(self):
        return '\n'.join(map(str, self.rows))

class Row:
    def __init__(self, tablename, record):
        self.values = dict()
        for c, v in record.items():
            self.values[(tablename, c)] = v 
    
    def __str__(self):
        return ' '.join(map(str, self.values.items()))

class Operator:
    def __init__(self, dataset):
        self.dataset = dataset
        self.idx = -1

    def next(self):
        if self.idx == self.dataset.size - 1:
            return -1
        else:
            self.idx += 1
            return self.dataset.getRow(self.idx)
