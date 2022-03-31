-- NONCDB
SET SQLFORMAT csv 
SPOOL 'output/sched-jobs.csv';
select owner,job_name,program_owner,program_name,job_type,
        start_date,to_char(repeat_interval) repeat_interval,enabled,state,run_count,failure_count,
        last_start_date,extract(day from last_run_duration*86400) last_run_duration,next_run_date
        from dba_scheduler_jobs  ;
SPOOL off;
SET SQLFORMAT ansiconsole