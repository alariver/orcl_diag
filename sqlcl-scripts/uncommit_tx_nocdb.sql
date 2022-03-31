-- CDB
SET SQLFORMAT csv 
SPOOL 'output/uncommit-tx.csv';
select s.inst_id, s.sid, start_time, username, r.name undo_name,  
            ubafil, ubablk, t.status, s.status sess_status,(used_ublk*p.value)/1024 blk, used_urec
            from gv$transaction t, v$rollname r, gv$session s, v$parameter p
            where xidusn=usn
            and s.saddr=t.ses_addr
            and p.name='db_block_size'
            order by 1;
SPOOL off;
SET SQLFORMAT ansiconsole