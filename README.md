# Cost-based optimization of relational databases

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
should be done. The executed queries are from file *queries.txt* (can be added, but after new line and the sql style has to match the sample ones). 
Running for the first time on particular data, make sure that the *stats* parameter in *params/engine_params.txt* is set to *on*. When running for the second time, it can be set to *off*, which will save a lot of time - data is the same, so info does not have to be gathered.
Also remember, that when the indexes are created, they will be found when running for the second time - to *forget* the index, use
```
make clean_index
```
