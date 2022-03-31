-- NONCDB
SET SQLFORMAT csv 
SPOOL 'output/db-auto-tasks.csv';
select t.client_name, t.task_name, t.operation_name, t.status, 
            w.window_group_name, w.enabled, w.NEXT_START_DATE
        from dba_autotask_task t, dba_autotask_client c, DBA_SCHEDULER_WINDOW_GROUPS w
        where  t.client_name = c.client_name
           and c.window_group = w.window_group_name  ;
SPOOL off;
SET SQLFORMAT ansiconsole