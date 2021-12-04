import algebra as ra
import functools as ft

# one general projection 
# one common selection
# from cross joins
def naivePlan(output, database):
    fields, predicates, tables = output.get()

    reads = [ra.Read(t, database) for t in tables]

    if len(reads) > 1:
        outer = ft.reduce(lambda x, y: ra.CrossProduct(x, y), reads)
    else:
        outer = reads[0]

    return ra.Projection(fields, ra.Selection(predicates, outer))

def executePlan(plan):
    return plan.execute()