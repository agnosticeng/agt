{{define "fetch_range"}}

create table range_{{.RANGE_START}}_{{.RANGE_END}} engine=Memory
as (
    select 
        `id`,
        `time`,
        `type`,
        `by`,
        `parent`,
        `kids`,
        `descendants`,
        `text`
    from url(
        'https://hacker-news.firebaseio.com/v0/item/{' || '{{.RANGE_START}}' || '..' || '{{.RANGE_END}}' ||'}.json',
        'JSON',
        '
            id UInt64,
            time Int64,
            type String,
            by String,
            parent Nullable(UInt64),
            kids Array(UInt64),
            descendants Nullable(Int64),
            text Nullable(String)
        '
    )
)

{{end}}