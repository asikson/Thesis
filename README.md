# Cost-based optimization of relational databases

## Generating data
To generate new database, some cleaning needs to be done:
```
make clean_database
make clean_stats
``` 
This erases old tables' files and statistics gathered about old data. Then in file *params/generator_params.txt* the generator parameters can be set and 
```
make generate
```
will create new database files.
