# Cost-based optimization of relational databases

This repo is an implementation of a custom database engine in Python, using Berkeley DB to store data. It handles basic SQL queries. The program contains:
- implementation of basic operators
- data statistics storage
- data generator
- cost and result size estimating mechanism for each operator
- different implementations of joins
- join ordering algorithm
- query execution planner

## Generating data
To generate new database, some cleaning needs to be done:
```
make clean_database
make clean_stats
``` 
This erases old tables' files and statistics gathered about old data. Then in file *params/generator_params.txt* the generator parameters can be set (table sizes, distributions, min/max values) and 
```
make generate
```
will create new database files.

## Testing
To run the program
```
make test
```
should be done. The executed queries are from file *queries.txt* (can be added, but after new line and the sql style has to match the sample ones - tables separated with commas, no quotations with string values etc.). 

Running for the first time on particular data, **make sure that the *stats* parameter in *params/engine_params.txt* is set to *on***. When running for the second time, it can be set to *off*, which will **save a lot of time** - data is the same, so info does not have to be gathered.

Also remember, that when the indexes are created, they will be found when running for the second time - to *forget* the index, use
```
make clean_index
```

## Statistics
Statistics parameters are in file *params/stats_params.txt*. It's possible to clean the stats by 
```
make clean_stats
```
however then remember to have them turned on while running the program.

## Tips
To see the result of the query, set the *printResult* parameter in *params/engine_params.txt* to *on*.

If the program is lacking memory, the buffers in *params/engine_params.txt* might need to be reduced.

If the alternative query plans should be presented, set the *alternativePlans* in *params/engine_params.txt* to *on* and pass the number of alternative plans to *numOfAlternative*.
