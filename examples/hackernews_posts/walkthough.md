# Building the HackerNews posts pipeline step by step

## How does AGT works ?

An AGT Pipeline is made out of a set of templatized SQL queries.
All data processing logic, from getting the input data from a source, to transoforing it, and writing it to
any destination, is expressed as an SQL queries.
The Pipeline sequence of operations and settings are configured in the pipeline.yaml file.

### Engine

The SQL queries that compose a Pipeline must be executed by a ClickHouse instance at some point, and that's 
what the engine does.
AGT supports 3 execution engines:
    - remote: SQL queries are executed in a existing ClickHouse cluster, reachable by the AGT process
    - local: SQL queries are executed by a ClickHouse server spawned and managed by the AGT process
    - chdb: SQL queries are exexcuted in a CHDB session, right in the same process as AGT

The engine is configured in the pipeline.yaml file. 

### Transformation logic

Each Pipeline step is expressed as a set of SQL queries.
The queries are templatized and can make use of so-called `vars`.
`vars` can be passed to the AGT process CLI at sartup time, but can also be dynamically
produced by each step during execution.
Steps last line of result set will automatically be available as `vars` for the next step's SQL queries.
Most of the time, steps will read from and write to temporary tables, and output some metadata as result set
for the next step.

## The HackerNews posts pipeline

### Engine configuration

The HackerNews pipeline will read it's input data from the Hacker News API and produce an 
Apache Iceberg table as output.
We will make use of the `Local` execution engine. Below is out configuration:

```yaml
Engine:
  Local:
    # Here you can instruct the Local engine to install any ClickHouse UDF bundle
    # so you can use these inside your queries.
    # Here, we make use of the `PLATFORM` variable exposed by AGT to target the right UDF binary for the
    # current platform.
    Bundles:
      - https://github.com/agnosticeng/icepq/releases/download/v0.1.2/icepq_clickhouse_udf_bundle_0.1.2_{{ .PLATFORM | default "linux_amd64_v3" }}.tar.gz 

    # You can set any ClickHouse setting here to ensure they are applied for every query
    Settings:
      send_logs_level: warning
      allow_experimental_dynamic_type: 1
      allow_experimental_json_type: 1
      enable_dynamic_type: 1
      enable_json_type: 1
      enable_named_columns_in_function_tuple: 1
      schema_inference_make_columns_nullable: auto
      remote_filesystem_read_prefetch: 0
      input_format_parquet_use_native_reader: 1
      output_format_parquet_string_as_string: 0
      output_format_parquet_use_custom_encoder: 1
      output_format_parquet_write_page_index: 1
      output_format_parquet_write_bloom_filter: 1
      output_format_parquet_parallel_encoding: 0
      max_execution_time: 3600
      min_os_cpu_wait_time_ratio_to_throw: 100
      max_os_cpu_wait_time_ratio_to_throw: 200
      s3_max_connections: 50
      
```

### Init

The `init` step is executed once at pipeline startup.
Here, we will use it to retrieve the lastest post id inserted in the destination table, 
to ensure the pipeline to recover from a failure.
As AGT itself is stateless, we will use the destination table to recover the latest state.

```sql
select 
    arrayMax(res.value[].upper)::UInt64 as INIT_START, 
    throwIf(res.error::String = 'table does not exist', 'table does not exists', 8888::Int16) as _1,
    throwIf(res.error::String <> '', res.error::String) as _2,
from (select icepq_field_bound_values(concat('s3:/', path('{{.`ICEBERG_DESTINATION_TABLE_LOCATION`}}')), 'id') as res) 
settings allow_custom_error_code_in_throwif=true
```

This Pipeline make use of the `ICEBERG_DESTINATION_TABLE_LOCATION` var.
This var's value will be the URL of an S3-compatible endpoint.
We make use the the `icepq_field_bound_values` function, which is an user-defined function from the [`icepq`](https://github.com/agnosticeng/icepq/tree/main) project.
This function read bound values directly from Iceberg metadata files, allowing us to compute a max for the `id` column
without reading any Parquet file (fast).
This function take an URL with an `s3` scheme, so we do a quick string tranformation on the original `ICEBERG_DESTINATION_TABLE_LOCATION` value.
We also make use of [`throwIf`](https://clickhouse.com/docs/sql-reference/functions/other-functions#throwif) which we use 
as a kind of assert.
We return a specific error code when tha table does not exists yet which we will use later.

The query returns a single line result set containing a single column named `INIT_START`, this we will be able to
use this `var` in subsequent queries in the pipeline.
Note that any column stating with `_` will not be added to the `vars`.

### Source

Each row produced by the Source query will create a new task that will then flow through the Pipeline's stages.
Note that you can configure the Source to stop the AGT process on empty result set, or after a given ammount of runs.
Here, our Source query will procudes ranges of post ids. Each range will be a different task
that will then go through every stage of the Pipeline.

Le't see what the Source query looks like:

```sql
with
    {{.MAX_BATCH_SIZE | default "10"}} as max_batch_size,
    {{.MAX_BATCH_PER_RUN | default "100"}} as max_batch_per_run,
    (select * from url('https://hacker-news.firebaseio.com/v0/maxitem.json', 'Raw')) as tip,

    coalesce(
        {{.RANGE_END | toCH}} + 1,
        {{.INIT_START | toCH}},
        {{.DEFAULT_START | toCH}},
        1
    ) as start

select 
    generate_series as RANGE_START,
    least(tip::UInt64, (generate_series + max_batch_size - 1)::UInt64) as RANGE_END
from generate_series(
    assumeNotNull(start)::UInt64,
    assumeNotNull(tip)::UInt64,
    max_batch_size::UInt64
)
limit max_batch_per_run
```

As we want to produce valid post id ranges, we need to know what the maximum post id is before generating the ranges.
We use the following subquery to read the max post id directly from the HN API:

```sql
select * from url('https://hacker-news.firebaseio.com/v0/maxitem.json', 'Raw')
```

Then, we determine the correct start of the next set of ranges by checking the following vars in order:
    - `RANGE_END`: The last run of the Source query might have set this vars. If so, then our set of ranges will start at `RANGE_END`+1
    - `INIT_START`: If this is the first Source query run, then `RANGE_END` is not set, but the Init query might have set the `INIT_START` var.
    - `DEFAULT_START`: If `RANGE_END` and `INIT_START` are not set, the user might have provided the `DEFAULT_START` var at AGT startup time, then we use it.
    - 1: If nothing else is set, we will start from id 1.

The last part of the Source query will generate a set of ranges between `start` and `tip` respecting parameters `MAX_BATCH_SIZE` and `MAX_BATCH_PER_RUN`.

The Source will create a set of tasks with vars looking like:

```json
{
  "RANGE_START": 100,
  "RANGE_END": 109
}
```

### Stages

#### 1

The first Stage will be an `Execute` one. 
It will execute the `fetch_range` query on `PoolSize` tasks in parallel, and yield the resulting
tasks in the same order as the input (thanks to `Ordered` parameter).

Here is the `fetch_range` query:

```sql
create table range_{{.RANGE_START}}_{{.RANGE_END}} engine=Memory
as (
    select 
        `id`,
        `time`,
        `type`,
        `by`,
        `parent`,
        `kids`,
        `descendants`,
        `text`
    from url(
        'https://hacker-news.firebaseio.com/v0/item/{' || '{{.RANGE_START}}' || '..' || '{{.RANGE_END}}' ||'}.json',
        'JSON',
        '
            id UInt64,
            time Int64,
            type String,
            by String,
            parent Nullable(UInt64),
            kids Array(UInt64),
            descendants Nullable(Int64),
            text Nullable(String)
        '
    )
)
```

It makes use of the ClickHouse [`url`](https://clickhouse.com/docs/sql-reference/table-functions/url) table function
to fetch a range of posts from the API and put the result in a `Memory` table named after
the `RANGE_START` and `RANGE_END` variables.
This query does not have any result set, so the `vars` of the task will not be modified.
The shape of the vars will still be :

```json
{
  "RANGE_START": 100,
  "RANGE_END": 109
}
```

### 2

The next stage is a bit more complicated. It's a Buffer one.
It looks like this:

```yaml
Buffer:
  Enter: create_buffer
  Leave: rename_buffer
  Condition: condition
  Queries:
    - insert_into_buffer
    - drop_range
    - merge_vars
  MaxDuration: 5s
```

Buffer stages, as their named indicate, are meant to collapse multiple tasks together.
Here we do it to prevent too small and frequent Iceberg table insertions.

Buffer follows the following logic:
  - A Buffer stage starts with an empty state. 
  - The first time it sees a task, it will execute the `Enter` query with the first task's vars.
  - For each task flowing in, it will execute all the `Queries`, then the `Condition` query
  - If the `Condition` query returns 0 or if `MaxDuration` has been reached
    - It executes the `Leave` query
    - It reset to an empty state

#### Queries

##### `create_buffer`

```sql
create table buffer as range_{{.RANGE_START}}_{{.RANGE_END}}
engine = MergeTree 
order by {{.ORDER_BY}}
settings old_parts_lifetime=10
```

The `create_buffer` query is executed when the `Buffer` state is empty.
Here, we create a MergeTree table with a specific `order by` clause that
will server as a buffer for our posts ranges tasks.
The schema for this table is copied from the `range_*` table created by the previous stage.

##### `insert_into_buffer`

```sql
insert into table buffer
select * from range_{{.RIGHT.RANGE_START}}_{{.RIGHT.RANGE_END}}
````

For each input task, we insert rows from the `range_*` table to the buffer table.

##### `drop_range`

```sql
drop table range_{{.RIGHT.RANGE_START}}_{{.RIGHT.RANGE_END}} sync
```

After we copied our `range_*` table into our buffer table, we can now drop it
to free memory.

##### `merge_vars`

```sql
select
    least(
        {{.LEFT.RANGE_START | default "18446744073709551615"}}, 
        {{.RIGHT.RANGE_START}}
    ) as RANGE_START,
    greatest(
        {{.LEFT.RANGE_END | default "0"}}, 
        {{.RIGHT.RANGE_END}}
    ) as RANGE_END,
    generateUUIDv7() || '.parquet' as OUTPUT_FILE
```

We need to modify our vars to account for the new posts id ranges we inserted into the buffer.
We will thus set new values for `RANGE_START` and `RANGE_END` vars by doing a min/max between 
previous and new range values.
We also create a new var named `OUTPUT_FILE` that we will use in the downstream stage.

##### `condition`

```sql
select toUInt64(count(*)) >= {{.MAX_BUFFER_SIZE | default "1000"}} as value from buffer
```

The condition query to leave the `Buffer` stage is rather simple, we just check if the buffer 
size has reached a given size.

##### `rename_buffer`

```sql
rename buffer to buffer_{{.RANGE_START}}_{{.RANGE_END}}
```

When the condition is false or `MaxDuration` is reached, we then execute a simple
query that justs rename our current buffer based on the new range values.

### 3

This stage is an Execute one, with no parallelism, and is reponsible for creating a new 
Parquet file in s3 based on our buffer table, adding it to the Iceberg table and doing some cleanup.

#### Queries

##### `write_parquet_file`

```sql
insert into function s3('{{.ICEBERG_DESTINATION_TABLE_LOCATION}}/data/{{.OUTPUT_FILE}}')
select * from buffer_{{.RANGE_START}}_{{.RANGE_END}}
order by {{.ORDER_BY}}
```

We use the [s3](https://clickhouse.com/docs/sql-reference/table-functions/s3) table function to
write the content of our buffer table to a Parquet file.

##### `iceberg_commit`

```sql
select icepq_add(concat('s3:/', path('{{.ICEBERG_DESTINATION_TABLE_LOCATION}}')), ['{{.OUTPUT_FILE}}'])
````

We use the `icepq_add` UDF from the [icepq](https://github.com/agnosticeng/icepq) project to add 
our newly generated Parquet file to the Iceberg table.

##### `drop_buffer``

```sql
drop table buffer_{{.RANGE_START}}_{{.RANGE_END}} sync
```

We just drop the buffer table once it's not needed anymore.