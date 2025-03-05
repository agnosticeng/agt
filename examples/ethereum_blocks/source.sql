with 
    {{.MAX_BATCH_SIZE | default "100"}} as max_batch_size,
    {{.MAX_BATCH_PER_RUN | default "10"}} as max_batch_per_run,

    (select evm_hex_decode_int(
        JSONExtract(
            ethereum_rpc(
                'eth_getBlockByNumber', 
                ['"{{.LATEST_BLOCK_STATUS}}"', 'false'], 
                '{{.RPC_ENDPOINT}}#fail-on-error=true&fail-on-null=true'
            ), 
            'value',
            'number',
            'String'
        ), 
        'UInt64'
    )) as tip,    

    if(
        {{.RANGE_END | default "null"}} is null,
        (select maxOrNull(number) from sink),
        {{.RANGE_END | default 0}}
    ) as prev_end,

    if (
        prev_end is null,
        0,
        prev_end + 1
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