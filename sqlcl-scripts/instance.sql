SET SQLFORMAT csv 
SPOOL 'output/instance.csv';
SELECT
  *
FROM gv$instance;
SPOOL off;
SET SQLFORMAT ansiconsole