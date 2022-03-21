SET SQLFORMAT csv 
SPOOL 'output/database.csv';
SELECT
  *
FROM v$database;
SPOOL off;
SET
  SQLFORMAT ansiconsole