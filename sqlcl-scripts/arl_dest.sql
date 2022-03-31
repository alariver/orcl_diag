SET SQLFORMAT csv 
SPOOL 'output/arl-dest-info.csv';
select i.instance_name, d.dest_name, 'status' metric, to_char(d.status) value             from gv$archive_dest d
            , gv$instance i
            , v$database db
            where d.status != 'INACTIVE'
            and   d.inst_id = i.inst_id
            and db.log_mode = 'ARCHIVELOG'
          union all
          select  i.instance_name, d.dest_name, 'sequence#' sequence ,to_char(d.log_sequence)
            from gv$archive_dest d
            , gv$instance i
            , v$database db
            where d.status != 'INACTIVE'
            and   d.inst_id = i.inst_id
            and db.log_mode = 'ARCHIVELOG'
          union all
          select  i.instance_name, d.dest_name , 'error' error, to_char(d.error)
            from gv$archive_dest d
            , gv$instance i
                , v$database db
            where d.status != 'INACTIVE'
            and   d.inst_id = i.inst_id
            and db.log_mode = 'ARCHIVELOG';
SPOOL off;
SET SQLFORMAT ansiconsole