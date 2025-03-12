insert into function s3(
    '{{.ICEBERG_TABLE_LOCATION}}/data/{{.OUTPUT_FILE}}',
    '{{.S3_ACCESS_KEY_ID}}',
    '{{.S3_SECRET_ACCESS_KEY}}'
)
select * from merge(arrayStringConcat({{.BUFFERS | toCH }}, '|'))
{{if .ORDER_BY}}
order by {{.ORDER_BY}}
{{end}}