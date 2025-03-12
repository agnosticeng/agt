select
    arrayPushBack(
        {{.LEFT.MERGES | default list | toCH}},
        arrayConcat(
            [{{.RIGHT.OUTPUT_FILE | toCH}}],
            {{.RIGHT.INPUT_FILES | toCH}}
        )
    ) as MERGES