from sqlparse.tokens import Newline
import algebra as ra
import functools as ft
import output as out
import info
from math import inf

class Plan:
    def __init__(self, queryOutput, acc):
        self.fields, self.predicates, self.tables = queryOutput.get()
        self.acc = acc

    def printResult(self, result):
        numOfRecords = 0
        for rec in result:
            print(rec)
            numOfRecords += 1
        print('Cost: ' + str(result.cost))
        print('Number of records: ', numOfRecords)

    def listDiff(self, list1, list2):
        result = []
        for e in list1:
            if e not in list2:
                result.append(e)

        return result

    def matchFields(self, predicate, table1, table2):
        field1, field2 = predicate.left, predicate.right
        if field1.tablename == table1.name:
            return field1, field2
        else:
            return field2, field1

    def matchField(self, predicate, table):
        field1, field2 = predicate.left, predicate.right
        if field1.tablename == table.name:
            return field1, field2
        else:
            return field2, field1

    def singleRelationPreds(self, table):
        chosen = list(filter(
            lambda p: p.withValue and p.left.tablename == table.name, 
            self.predicates))
        not_chosen = list(filter(
            lambda p: p not in chosen, self.predicates))

        return chosen, not_chosen

    def commonPredicates(self, table1, table2):
        names = [table1.name, table2.name]
        chosen = list(filter(
            lambda p: not p.withValue and p.left.tablename in names
                and p.right.tablename in names,
            self.predicates))
        
        not_chosen = list(filter(
            lambda p: p not in chosen, self.predicates))

        return chosen, not_chosen

    def predicatesIncluding(self, table):
        names = list(map(lambda t: t.name, self.tables))
        chosen = list(filter(
            lambda p: not p.withValue and \
                p.left.tablename not in names and \
                p.right.tablename not in names,
            self.predicates))
        not_chosen = list(filter(
            lambda p: p not in chosen, self.predicates))

        return chosen, not_chosen

    def singleRelation(self, table):
        chosen, _ = self.singleRelationPreds(table)

        if chosen == []:
            return table, chosen, ra.Read(table)
        else:
            return table, chosen, ra.ReadWithSelection(table, chosen)

    def pass1(self):
        return [self.singleRelation(t) for t in self.tables]

    def pairEveryTwo(self, rels):
        pairs = []
        for i in range(len(rels) - 1):
            j = i + 1
            while j < len(rels):
                pairs.append((rels[i], rels[j]))
                j += 1

        return pairs

    def twoRelationPlan(self, rel1, rel2):
        table1, chosen1, read1 = rel1
        table2, chosen2, read2 = rel2
        common, not_common = self.commonPredicates(table1, table2)
        preds_left = self.listDiff(not_common, chosen1 + chosen2)
        tables_left = self.listDiff(self.tables, [table1, table2])
        newOutput = out.evaluatorOutput(self.fields, preds_left, tables_left)

        if common == []:
            return Plan(newOutput, ra.CrossProduct(read1, read2))
        else:
            for c in common:
                f1, f2 = self.matchFields(c, table1, table2)
                if info.isForeinKey(table1.name, f1.name, table2.name, f2.name):
                    read2 = ra.ReadPkDict(table2)
                    predsForJoin = self.listDiff(common, [c]) + chosen2
                    return Plan(newOutput, ra.Join(read1, read2, predsForJoin, f1))
                elif info.isForeinKey(table2.name, f2.name, table1.name, f1.name):
                    read1 = ra.ReadPkDict(table1)
                    predsForJoin = self.listDiff(common, [c]) + chosen1
                    return Plan(newOutput, ra.Join(read2, read1, predsForJoin, f2))
            
            return Plan(newOutput, ra.Join(read1, read2, common, None))

    def pass2(self, rels):
        return [self.twoRelationPlan(p[0], p[1]) for p in self.pairEveryTwo(rels)]

    def joinTable(self, table):
        table, chosen, read = self.singleRelation(table)
        self.tables = self.listDiff(self.tables, [table])
        common, not_common = self.predicatesIncluding(table)
        preds_left = self.listDiff(self.predicates, common + chosen)
        newOutput = out.evaluatorOutput(self.fields, preds_left, self.tables)

        if common == []:
            return Plan(newOutput, ra.CrossProduct(self.acc, read))
        else:
            for c in common:
                f, fk = self.matchField(c, table)
                if info.isTablesPk(table.name, f):
                    read = ra.ReadPkDict(table)
                    predsForJoin = self.listDiff(common, [c]) + chosen
                    return Plan(newOutput, ra.Join(self.acc, read, predsForJoin, fk))
            
            return Plan(newOutput, ra.Join(self.acc, read, common, None))

    def multiFlatten(self, ls):
        result = []
        for e in ls:
            if isinstance(e, list):
                flattened = self.multiFlatten(e)
                result += flattened
            else:
                result.append(e)
        
        return result

    def rehash(self):
        if len(self.tables) == 1:
            result = self.joinTable(self.tables[0])
            if result.predicates != []:
                newAcc = ra.Projection(result.fields, 
                    ra.Selection(result.predicates, result.acc))
            else:
                newAcc = ra.Projection(result.fields, result.acc)
            
            emptyOutput = out.evaluatorOutput([], [], [])
            return Plan(emptyOutput, newAcc)
        else: 
            return list(map(lambda p: p.rehash(),
                [self.joinTable(t) for t in self.tables]))
    
    def estimation(self):
        singleRels = self.pass1()
        twoRelPlans = self.pass2(singleRels)

        rehashed = list(map(lambda p: p.rehash(), twoRelPlans))
        flattened = self.multiFlatten(rehashed)

        minCost = inf
        bestPlan = None
        for p in flattened:
            if p.acc.estCost < minCost:
                minCost = p.acc.estCost
                bestPlan = p
        
        print(bestPlan.acc)
        print('Estimated cost: ' + str(minCost))
        self.printResult(bestPlan.acc)
