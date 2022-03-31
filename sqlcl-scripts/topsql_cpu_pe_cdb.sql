SET SQLFORMAT csv 
SPOOL 'output/topsql-cpu-exec.csv';
select * from (select con_id ,sql_id, value,sql_text,row_number() over (partition by con_id order by value desc) count_sql 
                     from 
                    (
                    select con_id,sql_id,sql_text,sum(cpu_time/executions) value from gv$sql s
                    where  cpu_time > 0 and executions > 0
                    group by con_id,sql_id,sql_text
                    order by 4 desc, 1, 2, 3 
                    ))
                    where count_sql < 6;
SPOOL off;
SET SQLFORMAT ansiconsole