SET SQLFORMAT csv 
SPOOL 'output/cursor-efficiency.csv';
select a.inst_id,
        'session_cached_cursors'  parameter,
        lpad(value, 5)  value,
        decode(value, 0, '  n/a', to_char(100 * used / value, '990') || '%')  usage
        from
        ( select s.inst_id,
            max(s.value)  used
            from
            gv$statname  n,
            gv$sesstat  s
            where
            n.name = 'session cursor cache count' and
            s.statistic# = n.statistic#
            and s.inst_id = n.inst_id
            group by s.inst_id
        ) a,
        ( select inst_id,
            value
            from
            gv$parameter
            where
            name = 'session_cached_cursors'
        ) b
        where a.inst_id = b.inst_id
        union all
        select a.inst_id,
        'open_cursors',
        lpad(value, 5),
        to_char(100 * used / value,  '990') || '%'
        from
        ( select
        inst_id,
        max(used) used
        from (
            select
            s.inst_id,
            s.sid,(sum(s.value)) used
            from gv$statname n,
            gv$sesstat s
            where
            n.name in ('opened cursors current')
            and s.statistic# = n.statistic#
            and n.inst_id = s.inst_id
            group by
            s.inst_id,
            s.sid
        )
        group by
        inst_id
        ) a,
        ( select inst_id,
            value
            from
            gv$parameter
            where
            name = 'open_cursors'
        ) b
        where a.inst_id = b.inst_id;
SPOOL off;
SET SQLFORMAT ansiconsole