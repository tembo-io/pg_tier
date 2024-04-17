# pg_tier : Postgres extension for enabling data tiering to external storage
In today's data-driven landscape, managing vast amounts of information efficiently is paramount. Data has a lifecycle, right from the creation to deletion it goes through many stages e.g. when data is new it has high accessibility therefore it will be cached in-memory, we can consider this as hot data stage. Once the data gets older it will move to cold data stage then to archival stage or purge stage. Data access pattern, cost and resource constraints are some major factors that govern lifecyle of data.

This extension provides strategic solution to manage data that has lower access frequency, lower performance requirement and lower storage cost. It aligns with the concept of data lifecycle management, ensuring that data is stored cost-effectively while remaining accessible when needed.

# Installation

## Run with docker

Start the container

```bash
docker run -d -e POSTGRES_PASSWORD=postgres -p 5432:5432 --name pg-tier quay.io/tembo/tier-pg:latest
```

Then connect with `psql`

```bash
psql postgres://postgres:postgres@localhost:5432/postgres
```

## Load the extension

```sql
CREATE EXTENSION pg_tier CASCADE
```


# Usage

### Setup Credential

```sql
select tier.set_tier_credentials('S3_BUCKET_NAME','AWS_ACCESS_KEY', 'AWS_SECRET_KEY','AWS_REGION');
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