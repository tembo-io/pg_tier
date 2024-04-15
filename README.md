# pg_tier : Postgres extension for enabling data tiering to external storage
In today's data-driven landscape, managing vast amounts of information efficiently is paramount. Data has a lifecycle, right from the creation to deletion it goes through many stages e.g. when data is new it has high accessibility therefore it will be cached in-memory, we can consider this as hot data stage. Once the data gets older it will move to cold data stage then to archival stage or purge stage. Data access pattern, cost and resource constraints are some major factors that govern lifecyle of data.

This extension provides strategic solution to manage data that has lower access frequency, lower performance requirement and lower storage cost. It aligns with the concept of data lifecycle management, ensuring that data is stored cost-effectively while remaining accessible when needed.

# Installation

### Install dependency
Install extension `parquet_s3_fdw`. For details visit https://github.com/pgspider/parquet_s3_fdw

# Usage

### Load Extension

```sql
CREATE EXTENSION pg_tier;
```

### Setup Credential

```sql
select tier.set_tier_credentials('S3_BUCKET_NAME','AWS_ACCESS_KEY', 'AWS_SECRET_KEY','AWS_REGION');
```

### Enable tiered storage on a table

```sql
select tier.create_tier_table('TABLE_NAME');
```

### Tiering Data

```sql
select tier.execute_tiering('TABLE_NAME');
```

