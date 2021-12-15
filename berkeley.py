import berkeleydb as bdb

people = bdb.db()
people.open('people', dbtype=bdb.DB_HASH, flags=bdb.DB_CREATE)



