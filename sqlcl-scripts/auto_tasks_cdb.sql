-- CDB
SET SQLFORMAT csv 
SPOOL 'output/db-auto-tasks.csv';
select t.client_name, t.task_name, t.operation_name, t.status, t.con_id,
                w.window_group_name, w.enabled, w.NEXT_START_DATE
            from cdb_autotask_task t, cdb_autotask_client c, CDB_SCHEDULER_WINDOW_GROUPS w
            where t.con_id = c.con_id and t.client_name = c.client_name
            and c.con_id = w.con_id and c.window_group = w.window_group_name ;
SPOOL off;
SET SQLFORMAT ansiconsole