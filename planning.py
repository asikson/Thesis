import algebra as ra
import functools as ft

class Plan:
    def __init__(self, queryOutput, database, mode):
        self.fields, self.predicates, self.tables = queryOutput.get()
        self.database = database
        self.mode = mode
        self.tableDict = dict()
        for t in self.tables:
            self.tableDict[t.alias] = t

    def readTable(self, table):
        return ra.Read(table, self.database)

    def readIntoPkDict(self, table):
        return ra.ReadPkDict(table, self.database)

    def readTables(self, tables):
        return [self.readTable(t) for t in tables]

    def crossTables(self, tables):
        if len(tables) > 1:
            return ft.reduce(lambda x, y: ra.CrossProduct(x, y), tables)
        else:
            return tables[0]
    
    def joinTwoTables(self, left, right, predicate):
        return ra.Join(left, right, 
            predicate.left, predicate.right)

    def selectionPushdown(self):
        selections = []
        tablesLeft = []
        predsWithoutVal = list(filter(lambda p: not p.withValue, self.predicates))
        predsWithVal = list(filter(lambda p: p.withValue, self.predicates))

        for t in self.tables:
            preds = list(filter(lambda p: p.left.tablename == t.alias, predsWithVal))
            if preds == []:
                tablesLeft.append(t)
            else:
                selections.append(ra.Selection(preds, self.readTable(t)))
        
        return selections, tablesLeft, predsWithoutVal

    def applyJoins(self):
        predsWithoutVal = list(filter(lambda p: not p.withValue, self.predicates))
        predsWithVal = list(filter(lambda p: p.withValue, self.predicates))

        toJoin = [(self.tableDict[p.right.tablename], p) 
            for p in predsWithoutVal]
        
        tablesLeft = [t for t in self.tables 
            if t not in [p[0] for p in toJoin]]

        return toJoin, tablesLeft, predsWithVal


    def execute(self):
        if self.mode == 'cross_all':
            return self.executeCrossAll()
        elif self.mode == 'selection_pushdown':
            return self.executeSelectionPushdown()
        elif self.mode == 'apply_joins':
            return self.executeApplyJoins()
        else:
            print('Unknown mode')

    # crossing all tables and selecting rows
    def executeCrossAll(self):
        reads = self.readTables(self.tables)
        crosses = self.crossTables(reads)
        result = ra.Projection(self.fields, ra.Selection(self.predicates, crosses))

        for rec in result:
            print(rec)
        print('Cost of crossing all: ' + str(result.cost))

    # selection pushdown
    def executeSelectionPushdown(self):
        selections, self.tables, self.predicates = self.selectionPushdown()
        reads = self.readTables(self.tables)
        crosses = self.crossTables(reads + selections)

        result = ra.Projection(self.fields, 
            ra.Selection(self.predicates, crosses))

        for rec in result:
            print(rec)
        print('Cost of selection pushdown: ' + str(result.cost))

    # apply joins where possible
    def executeApplyJoins(self):
        toJoin, self.tables, self.predicates = self.applyJoins()
        reads = self.readTables(self.tables)
        crosses = self.crossTables(reads) 

        '''
        toJoin = list(map(lambda x: 
            (self.readIntoPkDict(x[0]), x[1]) if x[1].right.name == x[0].pk_name
            else (self.Read(x[0]), x[1]), toJoin))
        '''
        toJoin = list(map(lambda x: 
            (self.readIntoPkDict(x[0]), x[1]), toJoin))

        result = ra.Projection(self.fields, 
            ra.Selection(self.predicates,
                ft.reduce(lambda x, y: self.joinTwoTables(x, y[0], y[1]), toJoin, crosses)))

        for rec in result:
            print(rec)
        print('Cost of applying joins: ' + str(result.cost))