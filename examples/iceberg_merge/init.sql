create table iceberg_table_files engine=TinyLog
as (
    with 
        '{{.S3_ACCESS_KEY_ID}}' as s3_access_key_id,
        '{{.S3_SECRET_ACCESS_KEY}}' as s3_secret_access_key,
        '{{.TABLE_LOCATION}}' as table_location,
        '{{.MAX_MERGE_OUTPUT_SIZE | default "10737418240"}}' as max_merge_output_size

    select 
        _file as file,
        any(_size) as size,
        any(_time) as time
    from iceberg(table_location, s3_access_key_id, s3_secret_access_key)
    where _size < max_merge_output_size
    group by file
    order by time
)