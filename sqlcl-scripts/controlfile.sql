SET SQLFORMAT csv 
SPOOL 'output/controlfile.csv';
select * from v$controlfile;
SPOOL off;
SET SQLFORMAT ansiconsole