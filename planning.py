import datasets as ds
import algebra as ra

# one general projection 
# from one common selection
# from cross joins
def naivePlan(output, database):
    fields, predicates, tables = output.get()
    
    datasets = makeDatasets(tables, database)
    outerDataset = ra.Read(datasets[0])
    if len(datasets) > 1:
        i = 1
        while i < len(datasets):
            outerDataset = ra.CrossProduct(outerDataset, datasets[i])
            i += 1

    return ra.Projection(fields, ra.Selection(predicates, outerDataset))

def makeDatasets(tables, database):
    return [ds.Dataset(database.getTable(t.name), t.alias) for t in tables]