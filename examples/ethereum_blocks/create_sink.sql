create table sink as remote(
    '{{.CH_HOST}}', 
    {{.CH_DATABASE | default "default"}}, 
    {{.CH_TABLE}},
    '{{.CH_USER | default "default"}}',
    '{{.CH_PASSWD | default ""}}'
)