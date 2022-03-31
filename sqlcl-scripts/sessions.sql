SET SQLFORMAT csv 
SPOOL 'output/current-sessions.csv';
select * from gv$session where username is not null;
SPOOL off;
SET SQLFORMAT ansiconsole