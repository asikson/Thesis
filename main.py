import index as idx
import sql_plugin as sqlp
from sql_output import SqlOutput
import mystatistics as ms
import planning as pl
from algebra import getParam

if __name__ == '__main__':
    queries = sqlp.getQueries()
    statements = [sqlp.formatAndParse(q)
        for q in queries]
    outputs = list(map(
        lambda s: sqlp.evaluateStatement(s.tokens),
        statements))

    if getParam("stats") == "on":
        # statistics
        toStat = list(filter(
            lambda o: isinstance(o, SqlOutput),
            outputs))
        print('Preparing stats...')
        ms.prepareStats(toStat)
    else:
        print('Warning - stats are off!')
    print()

    alternative = getParam("alternativePlans")
    alternative = (alternative == "on")
    num = getParam("numOfAlternative")

    for s, out in zip(statements, outputs):
        print('SQL: ', s)
        if isinstance(out, SqlOutput):
            planner = pl.Planner(out)
            best = planner.getBest()
            planner.printBest()
            pl.printResult(best)

            if alternative:
                planner.printAlternative(num)

        elif isinstance(out, idx.Index) \
            and not idx.checkIfIndexExists(out.tablename, out.fields):
            
            out.createIndex()
            print('Created index {0} for {1} ({2})'.format(
                out.filename,
                out.tablename,
                ', '.join(out.fields)))
        print()
