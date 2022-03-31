SET SQLFORMAT csv 
SPOOL 'output/fra-info.csv';
select * from v$recovery_file_dest;
SPOOL off;
SET SQLFORMAT ansiconsole