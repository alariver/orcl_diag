SET SQLFORMAT csv 
SPOOL 'output/datafiles.csv';
select * from v$datafile;
SPOOL off;
SET SQLFORMAT ansiconsole