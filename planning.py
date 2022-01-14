import algebra as ra
import output as out
import struct_plugin as sp
import mystatistics as ms

class Plan:
    def __init__(self, queryOutput, acc):
        if queryOutput == -1:
            raise Exception('Wrong SQL!')
        if queryOutput is not None:
            self.fields, self.predicates, self.tables = queryOutput.get()
        self.acc = acc

    # tools
    def listDiff(self, list1, list2):
        result = []
        for e in list1:
            if e not in list2:
                result.append(e)

        return result

    def matchField(self, predicate, table):
        field1, field2 = predicate.left, predicate.right
        if field1.tablename == table.name:
            return field1, field2
        else:
            return field2, field1

    def multiFlatten(self, ls):
        result = []
        for e in ls:
            if isinstance(e, list):
                flattened = self.multiFlatten(e)
                result += flattened
            else:
                result.append(e)
        
        return result

    def pairEveryTwo(self, rels):
        pairs = []
        for i in range(len(rels) - 1):
            j = i + 1
            while j < len(rels):
                pairs.append((rels[i], rels[j]))
                j += 1

        return pairs


    # predicate recognizing
    def singleRelationPreds(self, table):
        return list(filter(
            lambda p: p.withValue and p.left.tablename == table.name, 
            self.predicates))

    def commonPredicates(self, table1, table2):
        names = [table1.name, table2.name]
        chosen = list(filter(
            lambda p: not p.withValue and p.left.tablename in names
                and p.right.tablename in names,
            self.predicates))

        return chosen

    def predicatesIncluding(self, table):
        names = self.listDiff(list(map(lambda t: t.name, self.tables)), [table.name])
        return list(filter(
            lambda p: not p.withValue and \
                (p.left.tablename == table.name or \
                    p.right.tablename == table.name) \
                and p.left.tablename not in names \
                and p.right.tablename not in names,
            self.predicates))

    def predicatesToApply(self):
        names = list(map(lambda t: t.name), self.tables)
        return list(filter(lambda p: not p.withValue
            and p.left.tablename not in names 
            and p.right.tablename not in names))


    # relations
    def singleRelation(self, table):
        chosen = self.singleRelationPreds(table)
        return table, chosen, ra.ReadWithSelection(table, chosen)

    def twoRelationPlan(self, rel1, rel2):
        table1, chosen1, read1 = rel1
        table2, chosen2, read2 = rel2
        tablesLeft = self.listDiff(self.tables, [table1, table2])
        commonPreds = self.commonPredicates(table1, table2)
        predsLeft = self.listDiff(self.predicates, chosen1 + chosen2 + commonPreds)
        newOutput = out.evaluatorOutput(self.fields, predsLeft, tablesLeft)

        if commonPreds == []:
            return Plan(newOutput, ra.CrossProduct(read1, read2))
        else:
            for c in commonPreds:
                f1, f2 = self.matchField(c, table1)
                if sp.isForeinKey(table1.name, f1.name, table2.name, f2.name):
                    read2 = ra.ReadPkDict(table2)
                    predsForJoin = self.listDiff(commonPreds, [c]) + chosen2
                    return Plan(newOutput, ra.Join(read1, read2, predsForJoin, f1))
                elif sp.isForeinKey(table2.name, f2.name, table1.name, f1.name):
                    read1 = ra.ReadPkDict(table1)
                    predsForJoin = self.listDiff(commonPreds, [c]) + chosen1
                    return Plan(newOutput, ra.Join(read2, read1, predsForJoin, f2))
            
            return Plan(newOutput, ra.Join(read1, read2, commonPreds, None))

    def joinTable(self, table):
        table, chosen, read = self.singleRelation(table)
        tablesLeft = self.listDiff(self.tables, [table])
        commonPreds = self.predicatesIncluding(table)
        predsLeft = self.listDiff(self.predicates, commonPreds + chosen)
        newOutput = out.evaluatorOutput(self.fields, predsLeft, tablesLeft)

        if commonPreds == []:
            return Plan(newOutput, ra.CrossProduct(self.acc, read))
        else:
            for c in commonPreds:
                f, fk = self.matchField(c, table)
                if sp.isTablesPk(table.name, f.name):
                    read = ra.ReadPkDict(table)
                    predsForJoin = self.listDiff(commonPreds, [c]) + chosen
                    return Plan(newOutput, ra.Join(self.acc, read, predsForJoin, fk))

            return Plan(newOutput, ra.Join(self.acc, read, commonPreds, None))

    def pass1(self):
        return [self.singleRelation(t) for t in self.tables]

    def pass2(self, rels):
        return [self.twoRelationPlan(p[0], p[1]) for p in self.pairEveryTwo(rels)]

    def rehash(self):
        if len(self.tables) == 0:
            newAcc = self.acc
            if self.predicates != []:
                newAcc = ra.Selection(self.predicates, newAcc)
            if self.fields != []:
                newAcc = ra.Projection(self.fields, newAcc)

            return Plan(None, newAcc)
        else: 
            return list(map(lambda p: p.rehash(),
                [self.joinTable(t) for t in self.tables]))


    # stats
    def prepareStats(self):
        stats = dict()
        for t in self.tables:
            stats[t.name] = []
        for p in self.predicates:
            if p.withValue:
                t, f, _, _ = p.split()
                stats[t].append(f)
            else:
                t1, f1, _, t2, f2 = p.split()
                stats[t1].append(f1)
                stats[t2].append(f2)

        for t, fields in stats.items():
            ms.createStatistics(t, fields)
            print('Stats for {0} prepared!'.format(t))


    # getting final result
    def bestPlans(self, n):
        # making stats
        self.prepareStats()

        print('Building plans...')
        singleRels = self.pass1()
        if len(singleRels) == 1:
            if self.fields == []:
                return [Plan(None, singleRels[0][2])]
            else:
                return [Plan(None, ra.Projection(self.fields, singleRels[0][2]))]
        twoRelPlans = self.pass2(singleRels)

        rehashed = list(map(lambda p: p.rehash(), twoRelPlans))
        flattened = self.multiFlatten(rehashed)
        print('Choosing best plans...')
        flattened = sorted(flattened, key=lambda p: p.acc.estCostCumulative)
        
        return flattened[:n]


def printResult(result):
    numOfRecords = 0
    for buffer in result:
        for rec in buffer:
            print(rec)
            numOfRecords += 1
    print('Cost: {0}'.format('{:.3f}'.format(
        result.costCumulative)))
    print('Number of records: ', numOfRecords)

def executePlan(plan):
    print('PLAN:')
    print(plan.acc)
    print('COSTS:')
    print('Estimated cost: {0}'.format(
        '{:.3f}'.format(plan.acc.estCostCumulative)))
    printResult(plan.acc)
    print()
