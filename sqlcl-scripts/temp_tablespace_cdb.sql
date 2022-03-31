SET SQLFORMAT csv 
SPOOL 'output/temp-tablespace-usage.csv';
select   t.con_id, t.TABLESPACE_NAME, 'filesize' param, t.totalspace value
                    from (select   round (sum (d.bytes))  AS totalspace,
                                round (sum ( case when maxbytes < bytes then bytes else maxbytes end)) max_bytes,
                                                    d.tablespace_name , d.con_id
                            from cdb_temp_files d
                        group by d.tablespace_name, d.con_id) t
                union all
                select   t.con_id, t.TABLESPACE_name ,'maxsize' param, sum(maxbytes) value
                        from (select case when autoextensible = 'NO'
                                            then bytes
                                    else
                                    case when bytes > maxbytes
                                            then bytes
                                    else          maxbytes
                                    end
                                    end maxbytes, tablespace_name, con_id
                                from cdb_temp_files) f
                            , cdb_tablespaces t
                    where t.contents = 'TEMPORARY'
                        and  t.tablespace_name = f.tablespace_name
                        and  t.con_id = f.con_id
                    group by t.con_id ,t.tablespace_name
                union all
                select t.con_id, t.tablespace_name , 'usedbytes' param, nvl(sum(u.blocks*t.block_size),0) value
                    from gv$sort_usage u right join
                    cdb_tablespaces t
                        on ( u.tablespace = t.tablespace_name and u.con_id = t.con_id)
                            where   t.contents = 'TEMPORARY'
                            group by t.con_id, t.tablespace_name
                union all
                select   t.con_id, t.TABLESPACE_name, 'pctfree' param, round(((t.totalspace - nvl(u.usedbytes,0))/t.totalspace)*100) value
                    from (select  d.con_id, round(sum (d.bytes))  AS totalspace,
                                round (sum ( case when maxbytes < bytes then bytes else maxbytes end)) max_bytes,
                                d.tablespace_name
                            from cdb_temp_files d
                        group by d.con_id, d.tablespace_name) t
                    left join (
                                        select u.con_id, u.tablespace tablespace_name, round(sum(u.blocks*t.block_size)) usedbytes
                                        from gv$sort_usage u
                                        , cdb_tablespaces t
                                        where u.tablespace = t.tablespace_name
                                        and   u.con_id = t.con_id
                                        and   t.contents = 'TEMPORARY'
                                        group by u.con_id, tablespace
                                ) u
                        on (t.tablespace_name = u.tablespace_name and t.con_id = u.con_id);
SPOOL off;
SET SQLFORMAT ansiconsole