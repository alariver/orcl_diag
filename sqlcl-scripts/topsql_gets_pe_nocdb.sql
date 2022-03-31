-- NO CDB
SET SQLFORMAT csv 
SPOOL 'output/topsql-gets-exec.csv';
select sql_id, value,sql_text from 
				(
				select sql_id,sql_text,sum(buffer_gets/executions) value from gv$sql s
                where  buffer_gets > 0 and executions > 0
				group by sql_id,sql_text
				order by 3 desc, 1, 2
				)
				where rownum < 6;
SPOOL off;
SET SQLFORMAT ansiconsole