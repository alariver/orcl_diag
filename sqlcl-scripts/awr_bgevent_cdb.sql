-- CDB
SET SQLFORMAT csv 
SPOOL 'output/awr-bg-event-wait.csv';
select * from (select con_id,
                    dbid,
                    snap_id,
                    instance_number,event_name state_name,
                    begin_interval_time,
                    end_interval_time,
                    wait_time,
                    lead(wait_time, 1, null) over (
                        partition by con_id,dbid,
                        instance_number,
                        event_name
                        order by
                        snap_id
                    ) - wait_time wait_time_delta,
                    variance(wait_time) over (partition by con_id,dbid, instance_number, event_name) waits_var
                    from (
                        select
                            stats.con_id,
                            stats.dbid,
                            stats.snap_id,
                            stats.instance_number,stats.event_name,
                            snap.begin_interval_time,
                            snap.end_interval_time,
                            sum(TIME_WAITED_MICRO) wait_time
                        from cdb_hist_bg_event_summary stats,
                            cdb_hist_snapshot snap
                        where
                            --latch.con_id = snap.con_id
                            stats.snap_id = snap.snap_id
                            and stats.dbid = snap.dbid
                            and snap.begin_interval_time >= sysdate - 7
                            and snap.end_interval_time <= sysdate
                            group by
                            stats.con_id,
                            stats.dbid,
                            stats.snap_id,
                            stats.instance_number,stats.event_name,
                            snap.begin_interval_time,
                            snap.end_interval_time
                            order by
                            stats.con_id,
                            stats.instance_number,
                            stats.event_name,
                            stats.snap_id
                    ) T)
                    where
                    1=1
                    and WAITS_VAR != 0;
SPOOL off;
SET SQLFORMAT ansiconsole