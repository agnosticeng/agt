insert into sink
select * from merge('{{.BUFFERS}}')
settings max_threads=1