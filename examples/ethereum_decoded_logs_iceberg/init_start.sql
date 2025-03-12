select 
    coalesce(
        maxOrNull(block_number) + 1,
        {{.DEFAULT_START}}
    ) as INIT_START
from iceberg(
    '{{.ICEBERG_TABLE_LOCATION}}',
    '{{.S3_ACCESS_KEY_ID}}',
    '{{.S3_SECRET_ACCESS_KEY}}'
)