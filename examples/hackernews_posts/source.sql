{{define "source"}}

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

{{end}}