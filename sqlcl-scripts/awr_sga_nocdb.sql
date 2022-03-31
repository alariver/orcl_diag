-- CDB
SET SQLFORMAT csv 
SPOOL 'output/awr-sga-stat.csv';
select * from (select 
                dbid,
                snap_id,
                instance_number,name stat_name,
                begin_interval_time,
                end_interval_time,
                wait_time,
                lead(wait_time, 1, null) over (
                    partition by dbid,
                    instance_number,
                    name
                    order by
                    snap_id
                ) - wait_time wait_time_delta,
                variance(wait_time) over (partition by dbid, instance_number, name) waits_var
                from (
                    select
                        stats.dbid,
                        stats.snap_id,
                        stats.instance_number,stats.name,
                        snap.begin_interval_time,
                        snap.end_interval_time,
                        sum(bytes) wait_time
                    from dba_hist_sgastat stats,
                        dba_hist_snapshot snap
                    where
                        stats.snap_id = snap.snap_id
                        and stats.dbid = snap.dbid
                        and snap.begin_interval_time >= SYSDATE - 7
                        and snap.end_interval_time <= SYSDATE
                        group by
                        stats.dbid,
                        stats.snap_id,
                        stats.instance_number,stats.name,
                        snap.begin_interval_time,
                        snap.end_interval_time
                        order by
                        stats.instance_number,
                        stats.name,
                        stats.snap_id
                ) T)
                where
                1=1
                and WAITS_VAR != 0;
SPOOL off;
SET SQLFORMAT ansiconsole