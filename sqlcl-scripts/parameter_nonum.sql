SET SQLFORMAT csv 
SPOOL 'output/parameter-nonnum.csv';
select i.instance_name , p.name , p.value
            from gv$instance i, gv$parameter p
            where i.instance_number = p.inst_id
            and   p.type not in (3,6) and p.isdefault = 'FALSE' ;
SPOOL off;
SET SQLFORMAT ansiconsole