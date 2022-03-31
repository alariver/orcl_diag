-- CDB
SET SQLFORMAT csv 
SPOOL 'output/awr-latches.csv';
select * from (select 
                dbid,
                snap_id,
                level#,latch_hash,instance_number,latch_name,
                begin_interval_time,
                end_interval_time,
                wait_time,
                lead(wait_time, 1, null) over (
                    partition by dbid,
                    instance_number,
                    latch_name
                    order by
                    snap_id
                ) - wait_time wait_time_delta,
                variance(wait_time) over (partition by dbid, instance_number, latch_name) waits_var
                from (
                    select
                latch.dbid,
                latch.snap_id,
                latch.level#,latch.latch_hash,latch.instance_number,latch.latch_name,
                snap.begin_interval_time,
                snap.end_interval_time,
                sum(wait_time) wait_time
                from dba_hist_latch latch,
                dba_hist_snapshot snap
                where
                latch.snap_id = snap.snap_id
                and latch.dbid = snap.dbid
                and snap.begin_interval_time >= sysdate - 7
                and snap.end_interval_time <= sysdate
                group by
                latch.dbid,
                latch.snap_id,
                latch.level#,latch.latch_hash,latch.instance_number,latch.latch_name,
                snap.begin_interval_time,
                snap.end_interval_time
                order by
                latch.instance_number,
                latch.latch_name,
                latch.snap_id
                ) T)
                where
                1=1
                and WAITS_VAR != 0;
SPOOL off;
SET SQLFORMAT ansiconsole