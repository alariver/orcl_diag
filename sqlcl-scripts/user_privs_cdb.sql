-- CDB
SET SQLFORMAT csv 
SPOOL 'output/user-privs.csv';
select u.con_id,	username, 
                    default_tablespace, 
                    temporary_tablespace,
                    granted_role, 
                    default_role 
                from cdb_users u,cdb_role_privs r
                where u.username = r.grantee(+)
                and u.con_id = r.con_id(+)
                order by con_id,username;
SPOOL off;
SET SQLFORMAT ansiconsole