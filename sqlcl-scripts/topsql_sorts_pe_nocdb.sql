-- NO CDB
SET SQLFORMAT csv 
SPOOL 'output/topsql-sort-exec.csv';
select sql_id, value,sql_text from 
				(
				select sql_id,sql_text,sum(sorts/executions) value from gv$sql s
                where  sorts > 0 and executions > 0
				group by sql_id,sql_text
				order by 3 desc, 1, 2
				)
				where rownum < 6;
SPOOL off;
SET SQLFORMAT ansiconsole