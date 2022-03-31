-- CDB
SET SQLFORMAT csv 
SPOOL 'output/sql-stat-hist.csv';
select * from (select 
                dbid,
                snap_id,
                instance_number,sql_id stat_name,
                begin_interval_time,
                end_interval_time,
                wait_time,
                lead(wait_time, 1, null) over (
                    partition by dbid,
                    instance_number,
                    sql_id
                    order by
                    snap_id
                ) - wait_time wait_time_delta,
                variance(wait_time) over (partition by dbid, instance_number, sql_id) waits_var
                from (
                    select
                        stats.dbid,
                        stats.snap_id,
                        stats.instance_number,stats.sql_id,
                        snap.begin_interval_time,
                        snap.end_interval_time,
                        sum(ELAPSED_TIME_TOTAL) wait_time
                    from dba_hist_sqlstat stats,
                        dba_hist_snapshot snap
                    where
                        stats.snap_id = snap.snap_id
                        and stats.dbid = snap.dbid
                        and snap.begin_interval_time >= SYSDATE - 7
                        and snap.end_interval_time <= SYSDATE
                        group by
                        stats.dbid,
                        stats.snap_id,
                        stats.instance_number,stats.sql_id,
                        snap.begin_interval_time,
                        snap.end_interval_time
                        order by
                        stats.instance_number,
                        stats.sql_id,
                        stats.snap_id
                ) T)
                where
                1=1
                and WAITS_VAR != 0;
SPOOL off;
SET SQLFORMAT ansiconsole