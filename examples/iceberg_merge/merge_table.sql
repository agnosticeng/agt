select icepq_merge(
    's3:/' || path('{{.TABLE_LOCATION}}'),
    {{.MERGES | toCH}}
)