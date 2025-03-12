select 
    toUInt64(count(*)) as size 
from merge(arrayStringConcat({{.BUFFERS | toCH }}, '|'))