select
    least(
        {{.LEFT.RANGE_START | default "18446744073709551615"}}, 
        {{.RIGHT.RANGE_START}}
    ) as RANGE_START,
    greatest(
        {{.LEFT.RANGE_END | default "0"}}, 
        {{.RIGHT.RANGE_END}}
    ) as RANGE_END,
    arrayPushBack(
        {{.LEFT.BUFFERS | default list | toCH}},
        'buffer_{{.RIGHT.RANGE_START}}_{{.RIGHT.RANGE_END}}'
    ) as BUFFERS,
    generateUUIDv7() || '.parquet' as OUTPUT_FILE