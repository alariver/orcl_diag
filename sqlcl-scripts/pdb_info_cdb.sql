SET SQLFORMAT csv 
SPOOL 'output/pdb-info.csv';
select
  *
from v$containers;
SPOOL off;
SET SQLFORMAT ansiconsole
