~ To be updated ~

# PostgreSQL

We use PostgreSQL version 17.2. The source models are written in AlchemySQL, 
therefore any other Database can be used.

## Tunnels 
For connecting from local PgAdmin to remote database

## Config
- Local (default): 5432
- Cluster: 5433
- Find ports, connection strings and database names [here](config/config.json).

## Local PostgreSQL (MacOS)

```bash
$ brew services start postgresql@17
$ brew services stop postgresql@17
$ lsof -i :5432 # Check status
```

## Cluster PostgreSQL

These variables have to be available to the environment:

```bash
$ export PATH=/vast/lu72hip/pgsql/bin:$PATH
$ export LD_LIBRARY_PATH=/vast/lu72hip/pgsql/lib:$LD_LIBRARY_PATH
$ export PGDATA=/vast/lu72hip/pgsql/data
```

Note: Port 5433 is configured in `$PGDATA/postgresql.conf`

```bash
$ pg_ctl -D $PGDATA start
$ pg_ctl -D $PGDATA stop
$ pg_ctl -D $PGDATA status
```

## Local SSH Tunnel

To connect to the cluster database from a local machine:

```bash
# Create tunnel on your local machine
$ ssh -N -L 5433:localhost:5433 lu72hip@login2.draco.uni-jena.de

# Check if tunnel is active
$ lsof -i :5433

# Stop tunnel
$ lsof -ti :5433 | xargs kill
```

## Dumps

To create a dump use the following command:

```bash
# Change $DB_NAME to the databases name
$ pg_dump -h localhost -p 5433 -U lu72hip $DB_NAME > backup_db_digicher.transformation

# You may omit tables using this before the $DB_Name
--exclude-table=researchoutputs

# Use pg_dumpall if you want to dump all databases
```