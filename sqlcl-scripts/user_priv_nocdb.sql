-- NON CDB
SET SQLFORMAT csv 
SPOOL 'output/user-privs.csv';
select 	username, 
                default_tablespace, 
                temporary_tablespace,
                granted_role, 
                default_role 
            from dba_users u,dba_role_privs r
            where u.username = r.grantee(+)
            order by username;
SPOOL off;
SET SQLFORMAT ansiconsole