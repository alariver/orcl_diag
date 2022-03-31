-- CDB
SET SQLFORMAT csv 
SPOOL 'output/awr-file-statx.csv';
select * from (select 
                dbid,
                snap_id,
                instance_number,filename stat_name,
                begin_interval_time,
                end_interval_time,
                wait_time,
                lead(wait_time, 1, null) over (
                    partition by dbid,
                    instance_number,
                    filename
                    order by
                    snap_id
                ) - wait_time wait_time_delta,
                variance(wait_time) over (partition by dbid, instance_number, filename) waits_var
                from (
                    select
                        stats.dbid,
                        stats.snap_id,
                        stats.instance_number,stats.filename,
                        snap.begin_interval_time,
                        snap.end_interval_time,
                        sum(time) wait_time
                    from DBA_HIST_FILESTATXS stats,
                        dba_hist_snapshot snap
                    where
                        stats.snap_id = snap.snap_id
                        and stats.dbid = snap.dbid
                        and snap.begin_interval_time >= SYSDATE - 7
                        and snap.end_interval_time <= SYSDATE
                        group by
                        stats.dbid,
                        stats.snap_id,
                        stats.instance_number,stats.filename,
                        snap.begin_interval_time,
                        snap.end_interval_time
                        order by
                        stats.instance_number,
                        stats.filename,
                        stats.snap_id
                ) T)
                where
                1=1
                and WAITS_VAR != 0;
SPOOL off;
SET SQLFORMAT ansiconsole