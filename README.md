
# pg_tier

**A Postgres extension to tier data to external storage**

## Beta Disclaimer

The features, functionality, and behavior of `pg_tier` are subject to change without notice. Updates and revisions may be released periodically as we work towards a stable version.

`pg_tier` may contain bugs, errors or other concurrency releated issues. It's not advised to use in production use-cases. We encourage you to test your use cases in the non-production environment. If you encounter any bugs, errors, or other issues, please report them. 

Your reports will help us improve the extension for the final release.

By proceeding with installation and use, you acknowledge that you have read, understood, and agree to the terms of this disclaimer.

## Overview

In today's data-driven landscape, managing vast amounts of information efficiently is paramount. Data has a lifecycle, right from the creation to deletion it goes through many stages e.g. when data is new it has high accessibility therefore it will be cached in-memory, we can consider this as hot data stage. Once the data gets older it will move to cold data stage then to archival stage or purge stage. Data access pattern, cost and resource constraints are some major factors that govern lifecyle of data.

This extension provides strategic solution to manage data that has lower access frequency, lower performance requirement and lower storage cost. It aligns with the concept of data lifecycle management, ensuring that data is stored cost-effectively while remaining accessible when needed.

## Installation

### Run with docker

Start the container

```bash
docker run -d -e POSTGRES_PASSWORD=postgres -p 5432:5432 --name pg-tier quay.io/tembo/tier-pg:latest
```

Then connect with `psql`

```bash
psql postgres://postgres:postgres@localhost:5432/postgres
```

### Load the extension

```sql
CREATE EXTENSION pg_tier CASCADE
```

## Usage

### Setup Credential

```sql
select tier.set_tier_credentials('my-storage-bucket','AWS_ACCESS_KEY', 'AWS_SECRET_KEY','AWS_REGION');
```

### Create a table

```sql
create table people (
    name text not null,
    age numeric not null
);
```

### Insert some data

```sql
insert into people values ('Alice', 34), ('Bob', 45), ('Charlie', 56);
```

### Enable tiered storage on the table

Initializes remote storage (S3) for the table.

```sql
select tier.create_tier_table('people');
```

### Tiering Data

Moves the local table into remote storage (S3).

```sql
select tier.execute_tiering('people');
```

### Query the remote table

```sql
select * from people;
```

```text
  name   | age
---------+-----
 Alice   |  34
 Bob     |  45
 Charlie |  56
```

### Table becomes foreign table with remote storage

```text
postgres=# \d people
                  Foreign table "public.people"
 Column |  Type   | Collation | Nullable | Default | FDW options
--------+---------+-----------+----------+---------+--------------
 name   | text    |           | not null |         | (key 'true')
 age    | numeric |           | not null |         | (key 'true')
Server: pg_tier_s3_srv
FDW options: (dirname 's3://my-storage-bucket/public_people/')
```

```text
postgres=# explain analyze select * from people;
                                               QUERY PLAN
---------------------------------------------------------------------------------------------------------
 Foreign Scan on people  (cost=0.00..0.09 rows=9 width=64) (actual time=126.438..126.444 rows=9 loops=1)
   Reader: Single File
   Row groups: 1
 Planning Time: 440.560 ms
 Execution Time: 172.527 ms
(5 rows)
```
