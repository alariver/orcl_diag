SET SQLFORMAT csv 
SPOOL 'output/redo-log.csv';
select l.group# group_id,l.thread# thread_id,bytes,archived,l.status,member from v$log l, v$logfile lf where l.group#=lf.group#;
SPOOL off;
SET SQLFORMAT ansiconsole