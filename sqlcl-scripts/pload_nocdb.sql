-- CDB
SET SQLFORMAT csv 
SPOOL 'output/database-perf-load.csv';
with sess_count_grpby_sec as (
                select to_char(sample_time,'YYYY-MM-DD HH24:MI:SS') sample_time_str, count(*) sess_count 
                from gv$active_session_history  
                where 1 = 1
                and sample_time >= sysdate - 7
                and sample_time <= sysdate
                group by to_char(sample_time,'YYYY-MM-DD HH24:MI:SS')
                union all
                select to_char(sample_time,'YYYY-MM-DD HH24:MI:SS') sample_time_str, count(*) sess_count 
                from dba_hist_active_sess_history 
                where 1 = 1
                and sample_time >= sysdate - 7
                and sample_time <= sysdate
                and dbid = (select dbid from v$database)
                group by to_char(sample_time,'YYYY-MM-DD HH24:MI:SS')
                )
                select substr(sample_time_str,1,15)||'0' ten_min_timestr, max(sess_count) sess_count
                from sess_count_grpby_sec
                group by  substr(sample_time_str,1,15)||'0' 
                order by  substr(sample_time_str,1,15)||'0' ;
SPOOL off;
SET SQLFORMAT ansiconsole