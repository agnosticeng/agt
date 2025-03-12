create table buffer_{{.RANGE_START}}_{{.RANGE_END}} engine=TinyLog
as (
    select
        t.* except (inputs),
        toJSONString(inputs) as inputs
    from remote(
        '{{.CH_HOST}}', 
        {{.CH_DATABASE | default "default"}}, 
        {{.CH_TABLE}},
        '{{.CH_USER | default "default"}}',
        '{{.CH_PASSWD | default ""}}'
    ) as t
    where block_number >= {{.RANGE_START}} and block_number <= {{.RANGE_END}}
)