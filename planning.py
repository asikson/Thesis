import algebra as ra
import struct_plugin as sp

class Planner:
    def __init__(self, sqlOutput):
        self.fields, self.predicates, \
            self.tables = sqlOutput.get()
        self.plans = []
        self.createPlans()

    def pass1(self):
        self.plans = [singleRelation(t, self.predicates) for t in self.tables]

    def twoRelationPlan(self, rel1, rel2):
        table1, chosen1, read1 = rel1
        table2, chosen2, read2 = rel2
        commonPreds = commonPredicates(table1, table2, self.predicates)
        
        fieldsLeft = self.fields
        tablesLeft = listDiff(self.tables, [table1, table2])
        predsLeft = listDiff(self.predicates, chosen1 + chosen2 + commonPreds)

        if commonPreds == []:
            return Plan(ra.CrossProduct(read1, read2),
                fieldsLeft, predsLeft, tablesLeft)
        else:
            for c in commonPreds:
                c.orderRightByTable(table2.name)
                f1, f2 = c.left, c.right

                if sp.isForeinKey(table1.name, f1.name, table2.name, f2.name):
                    read2 = ra.ReadPkDict(table2)
                    predsForJoin = listDiff(commonPreds, [c]) + chosen2
                    return Plan(ra.Join(read1, read2, predsForJoin, f1),
                        fieldsLeft, predsLeft, tablesLeft)
                    
                elif sp.isForeinKey(table2.name, f2.name, table1.name, f1.name):
                    read1 = ra.ReadPkDict(table1)
                    predsForJoin = listDiff(commonPreds, [c]) + chosen1
                    return Plan(ra.Join(read2, read1, predsForJoin, f2),
                        fieldsLeft, predsLeft, tablesLeft)
            
            return Plan(ra.Join(read1, read2, commonPreds, None),
                fieldsLeft, predsLeft, tablesLeft)
    
    def pass2(self):
        self.plans = [self.twoRelationPlan(p[0], p[1])
            for p in pairEveryTwo(self.plans)]

    def createPlans(self):
        self.pass1()
        if len(self.plans) == 1:
            relation = self.plans[0]

            if self.fields == []:
                self.plans[0] = Plan(relation[2], [], [], [])
            else:
                self.plans[0] = Plan(ra.Projection(relation[2]), self.fields, [], [])
        else:
            self.pass2()
            self.plans = list(map(lambda p: p.rehash(), self.plans))
            self.plans = multiFlatten(self.plans)
            self.plans = sorted(self.plans,
                key=lambda p: p.getEstCost())

    def getBest(self):
        return self.plans[0]

    def printBest(self):
        self.getBest().printPlan()


class Plan:
    def __init__(self, acc, fields, predicates, tables):
        self.acc = acc
        self.fields = fields
        self.predicates = predicates
        self.tables = tables

    def joinTable(self, table):
        table, chosen, read = singleRelation(table, self.predicates)
        commonPreds = predicatesToApply(table, self.tables, self.predicates)

        fieldsLeft = self.fields
        tablesLeft = listDiff(self.tables, [table])
        predsLeft = listDiff(self.predicates, commonPreds + chosen)

        if commonPreds == []:
            return Plan(ra.CrossProduct(self.acc, read),
                fieldsLeft, predsLeft, tablesLeft)
        else:
            for c in commonPreds:
                c.orderRightByTable(table.name)
                fk, f = c.left, c.right

                if sp.isTablesPk(table.name, f.name):
                    read = ra.ReadPkDict(table)
                    predsForJoin = listDiff(commonPreds, [c]) + chosen

                    return Plan(ra.Join(self.acc, read, predsForJoin, fk),
                        fieldsLeft, predsLeft, tablesLeft)

            return Plan(ra.Join(self.acc, read, commonPreds, None),
                fieldsLeft, predsLeft, tablesLeft)

    def rehash(self):
        if len(self.tables) == 0:
            newAcc = self.acc

            if self.predicates != []:
                newAcc = ra.Selection(self.predicates, newAcc)
            if self.fields != []:
                newAcc = ra.Projection(self.fields, newAcc)

            return Plan(newAcc, [], [], [])
        else: 
            return list(map(lambda p: p.rehash(),
                [self.joinTable(t) for t in self.tables]))

    def getEstCost(self):
        return self.acc.estCostCumulative
    
    def getCost(self):
        return self.acc.costCumulative

    def printPlan(self):
        print('PLAN:\n{0}'.format(self.acc))
        print('COSTS:\nEstimated cost: {0}'.format(
            '{:.3f}'.format(self.getEstCost())))


def printResult(plan):
    numOfRecords = 0
    for buffer in plan.acc:
        for rec in buffer:
            #print(rec)
            numOfRecords += 1
    print('Cost: {0}'.format(
        '{:.3f}'.format(plan.getCost())))
    print('Number of records: ', numOfRecords)


# tools
def listDiff(list1, list2):
    result = []
    for e in list1:
        if e not in list2:
            result.append(e)

    return result

def multiFlatten(ls):
    result = []
    for e in ls:
        if isinstance(e, list):
            flattened = multiFlatten(e)
            result += flattened
        else:
            result.append(e)
    
    return result
    
def pairEveryTwo(ls):
    pairs = []
    for i in range(len(ls) - 1):
        j = i + 1
        while j < len(ls):
            pairs.append((ls[i], ls[j]))
            j += 1

    return pairs

def singleRelation(table, predicates):
        chosen = singleRelationPreds(table, predicates)
        return table, chosen, ra.ReadWithSelection(table, chosen)

def singleRelationPreds(table, predicates):
    return list(filter(
        lambda p: p.withValue and p.left.tablename == table.name, 
        predicates))

def commonPredicates(table1, table2, predicates):
    names = [table1.name, table2.name]
    chosen = list(filter(
        lambda p: not p.withValue and p.left.tablename in names
            and p.right.tablename in names,
        predicates))

    return chosen

def predicatesToApply(table, tablesLeft, predicates):
    names = listDiff(
        list(map(lambda t: t.name, tablesLeft)),
        [table.name])

    return list(filter(
        lambda p: not p.withValue \
            and (p.left.tablename == table.name \
                or p.right.tablename == table.name) \
            and p.left.tablename not in names \
            and p.right.tablename not in names,
        predicates))