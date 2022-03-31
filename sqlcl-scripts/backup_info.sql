SET SQLFORMAT csv 
SPOOL 'output/backup-info.csv';
select operation,object_type,status,start_time,end_time from v$rman_status 
            where operation like 'BACKUP%'
            order by start_time;
SPOOL off;
SET SQLFORMAT ansiconsole