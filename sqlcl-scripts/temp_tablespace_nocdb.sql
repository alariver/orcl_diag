-- NO CDB
SET SQLFORMAT csv 
SPOOL 'output/temp-tablespace-usage.csv';
select    t.TABLESPACE_NAME, 'filesize' param, t.totalspace value
                from (select   round (sum (d.bytes))  AS totalspace,
                            round (sum ( case when maxbytes < bytes then bytes else maxbytes end)) max_bytes,
                                                d.tablespace_name 
                        from dba_temp_files d
                    group by d.tablespace_name) t
            union all
            select    t.TABLESPACE_name ,'maxsize' param, sum(maxbytes) value
                    from (select case when autoextensible = 'NO'
                                        then bytes
                                else
                                case when bytes > maxbytes
                                        then bytes
                                else          maxbytes
                                end
                                end maxbytes, tablespace_name
                            from cdb_temp_files) f
                        , dba_tablespaces t
                where t.contents = 'TEMPORARY'
                    and  t.tablespace_name = f.tablespace_name
                group by t.tablespace_name
            union all
            select  t.tablespace_name , 'usedbytes' param, nvl(sum(u.blocks*t.block_size),0) value
                from gv$sort_usage u right join
                dba_tablespaces t
                    on ( u.tablespace = t.tablespace_name )
                        where   t.contents = 'TEMPORARY'
                        group by  t.tablespace_name
            union all
            select    t.TABLESPACE_name, 'pctfree' param, round(((t.totalspace - nvl(u.usedbytes,0))/t.totalspace)*100) value
                from (select   round(sum (d.bytes))  AS totalspace,
                            round (sum ( case when maxbytes < bytes then bytes else maxbytes end)) max_bytes,
                            d.tablespace_name
                        from dba_temp_files d
                    group by  d.tablespace_name) t
                left join (
                                    select  u.tablespace tablespace_name, round(sum(u.blocks*t.block_size)) usedbytes
                                    from gv$sort_usage u
                                    , dba_tablespaces t
                                    where u.tablespace = t.tablespace_name
                                    and   t.contents = 'TEMPORARY'
                                    group by  tablespace
                            ) u
                    on (t.tablespace_name = u.tablespace_name );
SPOOL off;
SET SQLFORMAT ansiconsole