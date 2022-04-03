-- CDB
SET SQLFORMAT csv 
SPOOL 'output/ash-time-model.csv';
select con_id,to_char(trunc((sample_time),'HH'),'MM-DD HH24:MI') TM, state, count(*)/360 sess_cnt
                    from
                    (select  con_id, sample_time,   sample_id       
                    ,  CASE  WHEN session_state = 'ON CPU' THEN 'CPU'       
                            WHEN session_state = 'WAITING' AND wait_class IN ('User I/O') THEN 'IO'
                            WHEN session_state = 'WAITING' AND wait_class IN ('Cluster') THEN 'CLUSTER'
                            ELSE 'WAIT' END state            
                        from CDB_HIST_ACTIVE_SESS_HISTORY             
                        where   session_type IN ( 'FOREGROUND') 
                        and sample_time  between sysdate - 7 and sysdate  )
                    group by con_id, trunc((sample_time),'HH'), state order by trunc((sample_time),'HH');
SPOOL off;
SET SQLFORMAT ansiconsole