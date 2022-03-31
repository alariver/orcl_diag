SET SQLFORMAT csv 
SPOOL 'output/idle-events.csv';

select distinct name, wait_class from v$event_name ;

SPOOL off;
SET SQLFORMAT ansiconsole