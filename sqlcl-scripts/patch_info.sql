SET SQLFORMAT csv 
SPOOL 'output/patch-info.csv';
select
  ACTION || ':' || NAMESPACE || ':' || VERSION || ':' || ID || ':' || COMMENTS Patch_Info
from sys.registry$history
where
  action_time = (
    select
      max(action_time)
    from sys.registry$history
  );
SPOOL off;
SET SQLFORMAT ansiconsole