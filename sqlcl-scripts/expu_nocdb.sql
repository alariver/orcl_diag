-- NON-CDB
SET SQLFORMAT csv 
SPOOL 'output/expiring-user.csv';
select  username, (expiry_date - sysdate)*24*3600 expiring_secs
            from dba_users s
            where account_status IN ( 'OPEN', 'EXPIRED(GRACE)' )
            and expiry_date > sysdate
            and expiry_date < (sysdate + 30);
SPOOL off;
SET SQLFORMAT ansiconsole