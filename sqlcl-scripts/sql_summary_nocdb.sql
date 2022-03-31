SET SQLFORMAT csv 
SPOOL 'output/sqlarea-summary.csv';
select i.instance_name, count(1) total_sql,
                SUM(sharable_mem) totalSqlMem,
                SUM(DECODE(executions, 1, 1, 0)) singleUseSql,
                SUM(DECODE(executions, 1, sharable_mem, 0)) singleUseSqlMem,
                SUM(version_count) totalCurs,
                SUM(CPU_TIME) cpuTime,
                SUM(ELAPSED_TIME) elapseTime
                from gv$instance i,gv$sqlarea s
                where i.instance_number = s.inst_id
                group by i.instance_name;
SPOOL off;
SET SQLFORMAT ansiconsole