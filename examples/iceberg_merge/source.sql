with 
    {{.MAX_INPUT_FILES | default "10"}} as max_input_files,
    {{.MAX_MERGE_OUTPUT_SIZE | default "10737418240"}} as max_output_size, -- default=10GB

    q0 as (
        select
            *
        from iceberg_table_files
        {{if .FIRST_INPUT_FILE}}
        where file < '{{.FIRST_INPUT_FILE}}'
        {{end}}
    ),

    q1 as (
        select 
            groupArray(
                tuple(
                    file as file, 
                    size as size, 
                    time as time
                )
            ) over (order by time asc rows between current row and unbounded following) as input_files 
        from q0
    )

select 
    generateUUIDv7() || '.parquet' as OUTPUT_FILE,
    input_files[1].file as FIRST_INPUT_FILE,
    arrayMap(x -> x.file, input_files) as INPUT_FILES
from q1
where length(input_files) > 1
and length(input_files) <= max_input_files
and arraySum(arrayMap(x -> assumeNotNull(x.size), input_files))
limit 1

settings 
    enable_named_columns_in_function_tuple=1,
    max_threads=1
