SET SQLFORMAT csv 
SPOOL 'output/alert-dest.csv';
select i.instance_name, d.value||'/alert_'||i.instance_name||'.log' path 
            from gv$instance i, gv$diag_info d
           where i.inst_id = d.inst_id and d.name = 'Diag Trace';
SPOOL off;
SET   SQLFORMAT ansiconsole