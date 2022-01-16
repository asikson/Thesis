from index import Index
import sql_plugin as sqlp
from sql_output import SqlOutput
import mystatistics as ms
import planning as pl

if __name__ == '__main__':
    queries = sqlp.getQueries()
    statements = [sqlp.formatAndParse(q)
        for q in queries]
    '''
    for s in statements:
        s._pprint_tree()
    '''
    
    outputs = list(map(
        lambda s: sqlp.evaluateStatement(s.tokens),
        statements))

    # statistics
    toStat = list(filter(
        lambda o: isinstance(o, SqlOutput),
        outputs))
    print('Preparing stats...')
    ms.prepareStats(toStat)
    print()

    for out in outputs:
        if isinstance(out, SqlOutput):
            planner = pl.Planner(out)
            planner.printBest()
            best = planner.getBest()
            pl.printResult(best)
        elif isinstance(out, Index):
            print('Created index {0} for {1} ({2})'.format(
                out.tablename,
                out.filename,
                ', '.join(out.fields)))
            out.createIndex()


