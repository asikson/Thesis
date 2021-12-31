import algebra as ra
import functools as ft
import info
import statistics

class Plan:
    def __init__(self, queryOutput):
        self.fields, self.predicates, self.tables = queryOutput.get()

    def printResult(self, result):
        numOfRecords = 0
        for rec in result:
            print(rec)
            numOfRecords += 1
        print('Cost: ' + str(result.cost))
        print('Number of records: ', numOfRecords)

    def takePredsWithValue(self, table):
        predsChosen =  list(filter(
            lambda p: p.withValue and p.left.tablename == table.alias, 
            self.predicates))
        if predsChosen != []:
            self.predicates = list(filter(lambda p: p not in predsChosen))
        
        return predsChosen
    
    def takeCommonPredicates(self, table1, table2):
        aliases = [table1.alias, table2.alias]
        return list(filter(
            lambda p: not p.withValue and p.left.tablename in aliases
                and p.right.tablename in aliases,
            self.predicates))
 
    def getNecessaryFields(self, table):
        return list(filter(
            lambda f: f.tablename == table.alias,
            self.fields))
