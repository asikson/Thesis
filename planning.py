import algebra as ra
import functools as ft

class Plan:
    def __init__(self, queryOutput, database, mode):
        self.fields, self.predicates, self.tables = queryOutput.get()
        self.database = database
        self.mode = mode

    def planCrossAll(self):
        reads = self.readAllTables()
        if len(reads) > 1:
            return ft.reduce(lambda x, y: ra.CrossProduct(x, y), reads)
        else:
            return reads[0]
    
    def planJoin(self, left, table, field_1, field_2):
        return ra.Join(left, self.readIntoPkDict(table), field_1, field_2)

    def readTable(self, table):
        return ra.Read(table, self.database)

    def readAllTables(self):
        return [self.readTable(t) for t in self.tables]
    
    def readIntoPkDict(self, table):
        return ra.ReadPkDict(table, self.database)

    def execute(self):
        if self.mode == 'naive':
            return self.executeNaive()
        elif self.mode == 'join':
            return self.executeSimpleJoin()
        else:
            print('Unknown mode')

    # one general projection 
    # one common selection
    # from cross joins
    def executeNaive(self):
        crosses = self.planCrossAll()
        output = ra.Projection(self.fields, ra.Selection(self.predicates, crosses))

        result, cost = output.execute()
        print('Naive cost:', cost)

        return result

    def executeSimpleJoin(self):
        output = ra.Projection(self.fields, 
            self.planJoin(self.readTable(self.tables[0]), self.tables[1],
            self.predicates[0].left, self.predicates[0].right))

        result, cost = output.execute()
        print('Cost of simple join:', cost)

        return result

