select icepq_append(
    concat('s3:/', path('{{.ICEBERG_TABLE_LOCATION}}')),
    ['{{.OUTPUT_FILE}}']
)