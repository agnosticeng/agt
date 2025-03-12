with 
    {{.MAX_BATCH_SIZE | default "1000"}} as max_batch_size,
    {{.MAX_BATCH_PER_RUN | default "10"}} as max_batch_per_run,

    (
        select max(block_number) from remote(
            '{{.CH_HOST}}', 
            {{.CH_DATABASE | default "default"}}, 
            {{.CH_TABLE}},
            '{{.CH_USER | default "default"}}',
            '{{.CH_PASSWD | default ""}}'
        )
        where block_number > {{.TIP_MIN | default "0"}}
    ) as tip,

    (
        coalesce(
            {{.RANGE_END | default "null"}} + 1,
            {{coalesce .INIT_START .DEFAULT_START 0}}
        )
    ) as start

select 
    generate_series as RANGE_START,
    arrayMin([assumeNotNull(tip), toUInt64(generate_series + max_batch_size - 1)]) as RANGE_END
from generate_series(
    toUInt64(coalesce(start, 0)),
    assumeNotNull(tip),
    toUInt64(max_batch_size)
)
limit max_batch_per_run