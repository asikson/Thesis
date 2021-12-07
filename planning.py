import algebra as ra
import functools as ft

class Plan:
    def __init__(self, queryOutput, database, mode):
        self.fields, self.predicates, self.tables = queryOutput.get()
        self.reads = [ra.Read(t, database) for t in self.tables]

    @staticmethod
    def makeCross(self, reads):
        if len(reads) > 1:
            return ft.reduce(lambda x, y: ra.CrossProduct(x, y), reads)
        else:
            return reads[0]

    # one general projection 
    # one common selection
    # from cross joins
    def executeNaive(self):
        crosses = Plan.makeCross(self.reads)
        output = ra.Projection(self.fields, ra.Selection(self.predicates, crosses))
        self.execute(output)

    def execute(self, output):
        result, cost = output.execute()
        print('Full cost:', cost)

        return result