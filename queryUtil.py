import streamlit as st
import time
import pandas as pd
import numpy as np
import cx_Oracle
import sqlalchemy
import functools
import datetime

from functools import wraps

def lowdfcol(func):
    @wraps(func)
    def inner(*args, **kwargs):
        df = func(*args, **kwargs)
        df = df.rename(columns=str.lower)
        return df
    return inner


def translate_word(name):
    oracle_dictionary = {
        'instance': '实例',
        'name': '名称',
        'host': '主机',
        'version': '版本',
        'startup': '启动',
        'time': '时间',
        'status': '状态',
        'parallel': '并行',
        'patch': '补丁',
        'info': '信息',
        'memory': '内存',
        'component': '组件',
        'size': '大小',
        'parent': '父',
        'group': '组',
        'second': '第二',
        'parameter': '参数',
        'default': '默认',
        'order': '顺序',
        'minimum': '最小',
        'maximum': '最大',
        'path': '路径',
        'metric': '指标',
        'value': '值',
        'operation': '操作',
        'object': '对象',
        'type': '类型',
        'start': '开始',
        'end': '结束',
        'elapsed': '耗时',
        'con_id': '容器ID',
        'username': '用户名',
        'expiring': '将过期',
        'secs': '秒',
        'default':'默认',
        'tablespace': '表空间',
        'temporary': '临时',
        'granted': '被授予',
        'role': '角色',
        'thread_id': '实例ID',
        'bytes':'字节',
        'archived':'已归档',
        'member': '成员',
        'name_y': 'PDB名称',
        'name_x': '名称',
        'space': '空间',
        'free': '空闲',
        'usedbytes': '已用大小',
        'ten_min_timestr': '时间(10分钟)',
        'usage': '使用',
        'percent': '%'
        
    }
    if name.lower() in oracle_dictionary:
        return oracle_dictionary[name.lower()]
    result = []
    for item in name.split('_'):
        if item.lower() in oracle_dictionary:
            result.append(oracle_dictionary[item.lower()])
        else: 
            result.append(item)
    return '_'.join(result)

# @st.cache

@lowdfcol
def query_inst(target):
    engine = st.session_state.connections_defined[target]
    return pd.read_sql('select * from gv$instance', engine)


# @st.cache
@lowdfcol
def query_database(target):
    st.info(target)
    engine = st.session_state.connections_defined[target]
    st.write(engine)
    return pd.read_sql('select * from v$database', engine)

@lowdfcol
@functools.lru_cache()
def query_patchinfo(target):
    # engine = st.session_state.connections_defined[target]
    return pd.read_sql('''select action_time,  ACTION||':'||NAMESPACE||':'||VERSION||':'||ID||':'||COMMENTS Patch_Info
        from sys.registry$history order by action_time desc
        /* where action_time = (select max(action_time) from sys.registry$history) */''', st.session_state.connections_defined[target])

@lowdfcol
@functools.lru_cache()
def query_alert(target):
    engine = st.session_state.connections_defined[target]
    return pd.read_sql('''select i.instance_name, d.value||'/alert_'||i.instance_name||'.log' path 
            from gv$instance i, gv$diag_info d
           where i.inst_id = d.inst_id and d.name = 'Diag Trace' ''', engine)

@lowdfcol
def query_arl_dest(target):
    engine = st.session_state.connections_defined[target]
    return pd.read_sql('''select i.instance_name, d.dest_name, 'status' metric, to_char(d.status) value             from gv$archive_dest d
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
            and db.log_mode = 'ARCHIVELOG'     ''', engine)

@lowdfcol
@functools.lru_cache()
def query_fra(target):
    engine = st.session_state.connections_defined[target]
    return pd.read_sql('select * from v$recovery_file_dest', engine)

@lowdfcol
@functools.lru_cache()
def query_rman_bakinfo(target):
    engine = st.session_state.connections_defined[target]
    return pd.read_sql(''' select operation,object_type,status,start_time,end_time from v$rman_status 
            where operation like 'BACKUP%'
            order by start_time''', engine)

@lowdfcol
@functools.lru_cache()
def query_parameter(target):
    engine = st.session_state.connections_defined[target]
    return pd.read_sql('''select i.instance_name , p.name , p.value
            from gv$instance i, gv$parameter p
            where i.instance_number = p.inst_id
            and   p.type in (3,6) and p.isdefault = 'FALSE' ''', engine)

@lowdfcol
@functools.lru_cache()
def query_parameter2(target):
    engine = st.session_state.connections_defined[target]
    return pd.read_sql('''select i.instance_name , p.name , p.value
            from gv$instance i, gv$parameter p
            where i.instance_number = p.inst_id
            and   p.type not in (3,6) and p.isdefault = 'FALSE' ''', engine)

@lowdfcol
@functools.lru_cache()
def query_expu(target, is_cdb):
    engine = st.session_state.connections_defined[target]
    if is_cdb:
        sql = ''' select con_id, username, (expiry_date - sysdate)*24*3600 expiring_secs
            from cdb_users s
            where account_status IN ( 'OPEN', 'EXPIRED(GRACE)' )
            and expiry_date > sysdate
            and expiry_date < (sysdate + 30)'''
    else:
        sql = '''select  username, (expiry_date - sysdate)*24*3600 expiring_secs
            from dba_users s
            where account_status IN ( 'OPEN', 'EXPIRED(GRACE)' )
            and expiry_date > sysdate
            and expiry_date < (sysdate + 30)'''
    return pd.read_sql(sql, engine)

@lowdfcol
@functools.lru_cache()
def query_dbfile(target):
    engine = st.session_state.connections_defined[target]
    return pd.read_sql('select * from v$datafile', engine)

@lowdfcol
def query_pdb(target):
    engine = st.session_state.connections_defined[target]
    return pd.read_sql('select * from v$containers', engine)

@lowdfcol
@functools.lru_cache()
def query_pts(target,is_cdb):
    if is_cdb:
        sql = '''SELECT   t.con_id,t.tablespace_name
            ,        SUM(f.bytes) file_size
            ,        SUM(CASE
                            WHEN f.autoextensible = 'NO'
                            THEN f.bytes
                            ELSE GREATEST(f.bytes, f.maxbytes)
                        END) file_max_size
            ,        SUM(NVL(( SELECT   SUM(a.bytes)
                                FROM     cdb_free_space a
                                WHERE    a.tablespace_name = t.tablespace_name
                                AND      a.file_id         = f.file_id
                                AND      a.relative_fno    = f.relative_fno
                                and      a.con_id = f.con_id
                            ), 0)) file_free_space
            FROM     cdb_tablespaces t
            JOIN     cdb_data_files f
            ON     ( f.tablespace_name = t.tablespace_name and t.con_id = f.con_id)
            WHERE    t.CONTENTS = 'PERMANENT'
            GROUP BY t.con_id,t.tablespace_name
        '''
    else:
        sql = '''SELECT   t.tablespace_name
            ,        SUM(f.bytes) file_size
            ,        SUM(CASE
                            WHEN f.autoextensible = 'NO'
                            THEN f.bytes
                            ELSE GREATEST(f.bytes, f.maxbytes)
                        END) file_max_size
            ,        SUM(NVL(( SELECT   SUM(a.bytes)
                                FROM     dba_free_space a
                                WHERE    a.tablespace_name = t.tablespace_name
                                AND      a.file_id         = f.file_id
                                AND      a.relative_fno    = f.relative_fno
                            ), 0)) file_free_space
            FROM     dba_tablespaces t
            JOIN     dba_data_files f
            ON     ( f.tablespace_name = t.tablespace_name )
            WHERE    t.CONTENTS = 'PERMANENT'
            GROUP BY t.tablespace_name
        '''
    engine = st.session_state.connections_defined[target]
    return pd.read_sql(sql, engine)

@lowdfcol
@functools.lru_cache()
def query_uts(target,is_cdb):
    if is_cdb:
        sql = '''SELECT   t.con_id,t.tablespace_name
            ,        SUM(f.bytes) file_size
            ,        SUM(CASE
                            WHEN f.autoextensible = 'NO'
                            THEN f.bytes
                            ELSE GREATEST(f.bytes, f.maxbytes)
                        END) file_max_size
            ,        SUM(NVL(( SELECT   SUM(a.bytes)
                                FROM     cdb_free_space a
                                WHERE    a.tablespace_name = t.tablespace_name
                                AND      a.file_id         = f.file_id
                                AND      a.relative_fno    = f.relative_fno
                                and      a.con_id          = f.con_id
                            ), 0)) file_free_space
            FROM     cdb_tablespaces t
            JOIN     cdb_data_files f
            ON     ( f.tablespace_name = t.tablespace_name and f.con_id = t.con_id)
            WHERE    t.CONTENTS = 'UNDO'
            GROUP BY t.con_id, t.tablespace_name'''
    else:
        sql = '''SELECT   t.tablespace_name
         ,        SUM(f.bytes) file_size
         ,        SUM(CASE
                        WHEN f.autoextensible = 'NO'
                        THEN f.bytes
                        ELSE GREATEST(f.bytes, f.maxbytes)
                      END) file_max_size
         ,        SUM(NVL(( SELECT   SUM(a.bytes)
                            FROM     dba_free_space a
                            WHERE    a.tablespace_name = t.tablespace_name
                            AND      a.file_id         = f.file_id
                            AND      a.relative_fno    = f.relative_fno
                          ), 0)) file_free_space
         FROM     dba_tablespaces t
         JOIN     dba_data_files f
         ON     ( f.tablespace_name = t.tablespace_name )
         WHERE    t.CONTENTS = 'UNDO'
         GROUP BY t.tablespace_name'''
    engine = st.session_state.connections_defined[target]
    return pd.read_sql(sql, engine)

@lowdfcol
@functools.lru_cache()
def query_tts(target,is_cdb):
    if is_cdb:
        sql = '''select   t.con_id, t.TABLESPACE_NAME, 'filesize' param, t.totalspace value
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
                        on (t.tablespace_name = u.tablespace_name and t.con_id = u.con_id)'''
    else:
        sql = '''select    t.TABLESPACE_NAME, 'filesize' param, t.totalspace value
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
                    on (t.tablespace_name = u.tablespace_name )'''
    engine = st.session_state.connections_defined[target]
    return pd.read_sql(sql, engine)

@lowdfcol
@functools.lru_cache()
def query_sqlarea(target,is_cdb):
    if is_cdb:
        sql = '''select i.instance_name,s.con_id, count(1) total_sql,
                SUM(sharable_mem) totalSqlMem,
                SUM(DECODE(executions, 1, 1, 0)) singleUseSql,
                SUM(DECODE(executions, 1, sharable_mem, 0)) singleUseSqlMem,
                SUM(version_count) totalCurs,
                SUM(CPU_TIME) cpuTime,
                SUM(ELAPSED_TIME) elapseTime
                from gv$instance i,gv$sqlarea s
                where i.instance_number = s.inst_id
                group by i.instance_name, s.con_id'''
    else:
        sql = '''select i.instance_name, count(1) total_sql,
                SUM(sharable_mem) totalSqlMem,
                SUM(DECODE(executions, 1, 1, 0)) singleUseSql,
                SUM(DECODE(executions, 1, sharable_mem, 0)) singleUseSqlMem,
                SUM(version_count) totalCurs,
                SUM(CPU_TIME) cpuTime,
                SUM(ELAPSED_TIME) elapseTime
                from gv$instance i,gv$sqlarea s
                where i.instance_number = s.inst_id
                group by i.instance_name'''
    engine = st.session_state.connections_defined[target]
    return pd.read_sql(sql, engine)

@lowdfcol
@functools.lru_cache()
def query_topsql_generic(target, stat_column, is_cdb):
    if is_cdb:
        sql = f'''select * from (select con_id ,sql_id, value,sql_text,row_number() over (partition by con_id order by value desc) count_sql 
                     from 
                    (
                    select con_id,sql_id,sql_text,sum({stat_column}) value from gv$sql s
                    where {stat_column} > 0 and executions > 0
                    group by con_id,sql_id,sql_text
                    order by 4 desc, 1, 2, 3 
                    ))
                    where count_sql < 6'''
    else:
        sql = f'''select sql_id, value,sql_text from 
				(
				select sql_id,sql_text,sum({stat_column}) value from gv$sql s
                where {stat_column} > 0 and executions > 0
				group by sql_id,sql_text
				order by 3 desc, 1, 2
				)
				where rownum < 6'''
    engine = st.session_state.connections_defined[target]
    return pd.read_sql(sql, engine)

@lowdfcol
@functools.lru_cache()
def query_topsql_exec(target, is_cdb):
    return query_topsql_generic(target, 'executions', is_cdb)
    

@lowdfcol
@functools.lru_cache()
def query_topsql_sorts(target, is_cdb):
    return query_topsql_generic(target, 'sorts', is_cdb)
    

@lowdfcol
@functools.lru_cache()
def query_topsql_shm(target, is_cdb):
    return query_topsql_generic(target, 'sharable_mem', is_cdb)
    

@lowdfcol
@functools.lru_cache()
def query_topsql_pxexec(target, is_cdb):
    return query_topsql_generic(target, 'PX_SERVERS_EXECUTIONS', is_cdb)
    

@lowdfcol
@functools.lru_cache()
def query_topsql_invalid(target, is_cdb):
    return query_topsql_generic(target, 'invalidations', is_cdb)
    

@lowdfcol
@functools.lru_cache()
def query_topsql_parse(target, is_cdb):
    return query_topsql_generic(target, 'parse_calls', is_cdb)
    

@lowdfcol
@functools.lru_cache()
def query_topsql_phr(target, is_cdb):
    return query_topsql_generic(target, 'disk_reads', is_cdb)
    

@lowdfcol
@functools.lru_cache()
def query_topsql_get(target, is_cdb):
    return query_topsql_generic(target, 'buffer_gets', is_cdb)
    

# application wait time

@lowdfcol
@functools.lru_cache()
def query_topsql_awt(target, is_cdb):
    return query_topsql_generic(target, 'application_wait_time', is_cdb)
    

# concurrent wait time

@lowdfcol
@functools.lru_cache()
def query_topsql_conwt(target, is_cdb):
    return query_topsql_generic(target, 'concurrency_wait_time', is_cdb)
    

# cluster wait time


@lowdfcol
@functools.lru_cache()
def query_topsql_cluwt(target, is_cdb):
    return query_topsql_generic(target, 'cluster_wait_time', is_cdb)
    

# user io wait time

@lowdfcol
@functools.lru_cache()
def query_topsql_uiowt(target, is_cdb):
    return query_topsql_generic(target, 'user_io_wait_time', is_cdb)
    

# rows

@lowdfcol
@functools.lru_cache()
def query_topsql_rows(target, is_cdb):
    return query_topsql_generic(target, 'rows_processed', is_cdb)
    

# cpu time

@lowdfcol
@functools.lru_cache()
def query_topsql_cpu(target, is_cdb):
    return query_topsql_generic(target, 'cpu_time', is_cdb)
    

# elapse time

@lowdfcol
@functools.lru_cache()
def query_topsql_elps(target, is_cdb):
    return query_topsql_generic(target, 'elapsed_time', is_cdb)
    

# sorts per executions

@lowdfcol
@functools.lru_cache()
def query_topsql_sortpe(target, is_cdb):
    return query_topsql_generic(target, 'sorts/executions', is_cdb)
    

# parses per execution

@lowdfcol
@functools.lru_cache()
def query_topsql_parsepe(target, is_cdb):
    return query_topsql_generic(target, 'parse_calls/executions', is_cdb)
    

# buffer gets per exec

@lowdfcol
@functools.lru_cache()
def query_topsql_getpe(target, is_cdb):
    return query_topsql_generic(target, 'buffer_gets/executions', is_cdb)
    

# disk reads per execution

@lowdfcol
@functools.lru_cache()
def query_topsql_phrpe(target, is_cdb):
    return query_topsql_generic(target, 'disk_reads/executions', is_cdb)
    


# application wait time per exec
@lowdfcol
@functools.lru_cache()
def query_topsql_awtpe(target, is_cdb):
    return query_topsql_generic(target, 'application_wait_time/executions', is_cdb)
    

# concurrency wait time per exec

@lowdfcol
@functools.lru_cache()
def query_topsql_conwt_pe(target, is_cdb):
    return query_topsql_generic(target, 'concurrency_wait_time/executions', is_cdb)
    

# cluster wait time per exec

@lowdfcol
@functools.lru_cache()
def query_topsql_cluwt_pe(target,is_cdb):
    return query_topsql_generic(target, 'cluster_wait_time/executions', is_cdb)
    

# user io wait per exec

@lowdfcol
@functools.lru_cache()
def query_topsql_uiowt_pe(target,is_cdb):
    return query_topsql_generic(target, 'user_io_wait_time/executions', is_cdb)
    

# rows processed per exec

@lowdfcol
@functools.lru_cache()
def query_topsql_rows_pe(target, is_cdb):
    return query_topsql_generic(target, 'rows_processed/executions', is_cdb)
    

# cpu time per exec

@lowdfcol
@functools.lru_cache()
def query_topsql_cpu_pe(target, is_cdb):
    return query_topsql_generic(target, 'cpu_time/executions', is_cdb)
    

# elapsed time per exec

@lowdfcol
@functools.lru_cache()
def query_topsql_elps_pe(target, is_cdb):
    return query_topsql_generic(target, 'elapsed_time/executions', is_cdb)
    

@lowdfcol
@functools.lru_cache()
def query_fileio(target, is_cdb):
    if is_cdb:
        sql = '''select d.con_id,name file_name, PHYRDS, PHYWRTS,SINGLEBLKRDS,READTIM,WRITETIM,
                SINGLEBLKRDTIM,AVGIOTIM,LSTIOTIM,MINIOTIM,MAXIORTM,MAXIOWTM 
                from v$datafile d,v$filestat t
                where d.file# = t.file#'''
    else:
        sql = '''select name file_name, PHYRDS, PHYWRTS,SINGLEBLKRDS,READTIM,WRITETIM,
			SINGLEBLKRDTIM,AVGIOTIM,LSTIOTIM,MINIOTIM,MAXIORTM,MAXIOWTM 
			from v$datafile d,v$filestat t
			where d.file# = t.file#'''
    engine = st.session_state.connections_defined[target]
    return pd.read_sql(sql, engine)

@lowdfcol
@functools.lru_cache()
def query_tmpio(target, is_cdb):
    if is_cdb:
        sql = '''select d.con_id, name file_name, PHYRDS, PHYWRTS,SINGLEBLKRDS,READTIM,WRITETIM,
                SINGLEBLKRDTIM,AVGIOTIM,LSTIOTIM,MINIOTIM,MAXIORTM,MAXIOWTM 
                from v$tempfile d,v$tempstat t
                where d.file# = t.file#'''
    else:
        sql = '''select name file_name, PHYRDS, PHYWRTS,SINGLEBLKRDS,READTIM,WRITETIM,
			SINGLEBLKRDTIM,AVGIOTIM,LSTIOTIM,MINIOTIM,MAXIORTM,MAXIOWTM 
			from v$tempfile d,v$tempstat t
			where d.file# = t.file#'''
    engine = st.session_state.connections_defined[target]
    return pd.read_sql(sql, engine)

@lowdfcol
@functools.lru_cache()
def query_controlfile(target):
    sql = 'select * from v$controlfile'
    engine = st.session_state.connections_defined[target]
    return pd.read_sql(sql, engine)

@lowdfcol
@functools.lru_cache()
def query_logfile(target):
    sql = 'select l.group# group_id,l.thread# thread_id,bytes,archived,l.status,member from v$log l, v$logfile lf where l.group#=lf.group#'
    engine = st.session_state.connections_defined[target]
    return pd.read_sql(sql, engine)

@lowdfcol
@functools.lru_cache()
def query_user_privs(target, is_cdb):
    if is_cdb:
        sql = '''select u.con_id,	username, 
                    default_tablespace, 
                    temporary_tablespace,
                    granted_role, 
                    default_role 
                from cdb_users u,cdb_role_privs r
                where u.username = r.grantee(+)
                and u.con_id = r.con_id(+)
                order by con_id,username'''
    else:
        sql = '''select 	username, 
                default_tablespace, 
                temporary_tablespace,
                granted_role, 
                default_role 
            from dba_users u,dba_role_privs r
            where u.username = r.grantee(+)
            order by username'''
    engine = st.session_state.connections_defined[target]
    return pd.read_sql(sql, engine)

@lowdfcol
def query_curr_sessions(target, status=None):
    sql = '''select * from gv$session where username is not null '''
    if status:
        sql += " and status = '{}'".format(status.upper())
    engine = st.session_state.connections_defined[target]
    return pd.read_sql(sql, engine)

@lowdfcol
def query_cursor_efficiency(target):
    sql = """select a.inst_id,
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
        where a.inst_id = b.inst_id
    """
    engine = st.session_state.connections_defined[target]
    return pd.read_sql(sql, engine)

@lowdfcol
def query_pin_plsql(target):
    sql = """select * from (select con_id, owner, name, type, sum(sharable_mem) sharable_mem
            from gv$db_object_cache
            where sharable_mem > 1000 and executions > 10 and owner not in  ('SYS','MDSYS','CTXSYS','DBSNMP')
            and (type='PACKAGE' or type = 'PACKAGE BODY' or type='FUNCTION'
            or type = 'PROCEDURE') and kept= 'NO'
            group by con_id, owner, name, type
            order by sharable_mem desc) where rownum < 16"""
    engine = st.session_state.connections_defined[target]
    return pd.read_sql(sql, engine)

@lowdfcol
def query_pin_sql(target):
    sql = """select * from (select con_id,sql_text, sum(sharable_mem) sharable_mem, sum(invalidations) invalidations, sum(loads) loads
                from gv$sql
                where loads > invalidations
                and sharable_mem > 190000
                group by con_id,sql_text
                order by loads desc) where rownum < 16"""
    engine = st.session_state.connections_defined[target]
    return pd.read_sql(sql, engine)

@lowdfcol
def query_redundent_idx(target):
    sql = """select
            o1.name||'.'||n1.name  redundant_index,
            o2.name||'.'||n2.name  sufficient_index
            from
            sys.icol$  ic1,
            sys.icol$  ic2,
            sys.ind$  i1,
            sys.obj$  n1,
            sys.obj$  n2,
            sys.user$  o1,
            sys.user$  o2
            where
            ic1.pos# = 1 and
            ic2.bo# = ic1.bo# and
            ic2.obj# != ic1.obj# and
            ic2.pos# = 1 and
            ic2.intcol# = ic1.intcol# and
            i1.obj# = ic1.obj# and
            bitand(i1.property, 1) = 0 and
            ( select
                max(pos#) * (max(pos#) + 1) / 2
                from
                sys.icol$
                where
                obj# = ic1.obj#
            ) =
            ( select
                sum(xc1.pos#)
                from
                sys.icol$ xc1,
                sys.icol$ xc2
                where
                xc1.obj# = ic1.obj# and
                xc2.obj# = ic2.obj# and
                xc1.pos# = xc2.pos# and
                xc1.intcol# = xc2.intcol#
            ) and
            n1.obj# = ic1.obj# and
            n2.obj# = ic2.obj# and
            o1.user# = n1.owner# and
            o2.user# = n2.owner# 
            and o1.name not in ('SYS','DVSYS','MDSYS','XDB')"""
    engine = st.session_state.connections_defined[target]
    return pd.read_sql(sql, engine)

@lowdfcol
def query_low_density_idx(target):
    sql = """select /*+ ordered */
            u.name ||'.'|| o.name  index_name,
            substr(
                to_char(
                100 * i.rowcnt * (sum(h.avgcln+1) + 11) / (
                    i.leafcnt * (p.value - 66 - i.initrans * 24)
                ),
                '999.00'
                ),
                2
            ) || '%'  density,
            floor((1 - i.pctfree$/100) * i.leafcnt -
                i.rowcnt * (sum(h.avgcln+1) + 11) / (p.value - 66 - i.initrans * 24)
            ) extra_blocks
            from
            sys.ind$  i,
            sys.icol$  ic,
            sys.hist_head$  h,
            ( select
                kvisval  value
                from
                sys.x$kvis
                where
                kvistag = 'kcbbkl' )  p,
            sys.obj$  o,
            sys.user$  u
            where
            i.leafcnt > 1 and
            i.type# in (1,4,6) and		-- exclude special types
            ic.obj# = i.obj# and
            h.obj# = i.bo# and
            h.intcol# = ic.intcol# and
            o.obj# = i.obj# and
            o.owner# != 0 and
            u.user# = o.owner#
            group by
            u.name,
            o.name,
            i.rowcnt,
            i.leafcnt,
            i.initrans,
            i.pctfree$,
            p.value
            having
            50 * i.rowcnt * (sum(h.avgcln+1) + 11)
            < (i.leafcnt * (p.value - 66 - i.initrans * 24)) * (50 - i.pctfree$) and
            floor((1 - i.pctfree$/100) * i.leafcnt -
                i.rowcnt * (sum(h.avgcln+1) + 11) / (p.value - 66 - i.initrans * 24)
            ) > 0
            order by
            3 desc, 2"""
    engine = st.session_state.connections_defined[target]
    return pd.read_sql(sql, engine)

@lowdfcol
def query_row_migration_tab(target):
    sql = """select
            u.name ||'.'|| o.name  table_name,
            t.chncnt mrows,
            to_char(100 * t.chncnt / t.rowcnt, '9999.00') || '%'  migration,
            1 + greatest(
                t.pctfree$,
                ceil(
                ( 100 * ( p.value - 66 - t.initrans * 24 -
                        greatest(
                        floor(
                            (p.value - 66 - t.initrans * 24) / greatest(t.avgrln + 2, 11)
                        ) - 1,
                        1
                        ) * greatest(t.avgrln + 2, 11)
                    )
                    /
                    (p.value - 66 - t.initrans * 24)
                )
                )
            )  new_free,
            98 - greatest(
                t.pctfree$,
                ceil(
                ( 100 * ( p.value - 66 - t.initrans * 24 -
                        greatest(
                        floor(
                            (p.value - 66 - t.initrans * 24) / greatest(t.avgrln + 2, 11)
                        ) - 2,
                        1
                        ) * greatest(t.avgrln + 2, 11)
                    )
                    /
                    (p.value - 66 - t.initrans * 24)
                )
                )
            )  new_used,
            floor((p.value - 66 - t.initrans * 24) / greatest(t.avgrln + 2, 11))  minimize
            from
            sys.tab$  t,
            ( select
                obj#,
                sum(length)  max_row
                from
                sys.col$
                group by
                obj#
                having
                min(length) > 0		-- don't worry about tables with LONGs
            )  c,
            ( select
                kvisval  value
                from
                sys.x$kvis
                where
                kvistag = 'kcbbkl'
            )  p,
            sys.obj$  o,
            sys.user$  u
            where
            t.tab# is null and
            t.chncnt > 0 and
            t.rowcnt > 0 and
            c.obj# = t.obj# and
            c.max_row < p.value - (71 + t.initrans * 24) and
            o.obj# = t.obj# and
            o.owner# != 0 and
            u.user# = o.owner#
            order by
            2 desc"""
    engine = st.session_state.connections_defined[target]
    return pd.read_sql(sql, engine)

@lowdfcol
def query_tab_density(target):
    sql = """select /*+ ordered */
                u.name ||'.'|| o.name  table_name,
                lpad(decode(t.degree, 32767, 'DEFAULT', nvl(t.degree, 1)), 7)  degree,
                substr(
                    to_char(
                    100 * t.rowcnt / (
                        floor((p.value - 66 - t.initrans * 24) / greatest(t.avgrln + 2, 11))
                        * t.blkcnt
                    ),
                    '999.00'
                    ),
                    2
                ) ||
                '%'  density,
                1  new_free,
                99 - ceil(
                    ( 100 * ( p.value - 66 - t.initrans * 24 -
                        greatest(
                            floor(
                            (p.value - 66 - t.initrans * 24) / greatest(t.avgrln + 2, 11)
                            ) - 1,
                            1
                        ) * greatest(t.avgrln + 2, 11)
                    )
                    /
                    (p.value - 66 - t.initrans * 24)
                    )
                )  new_used,
                ceil(
                    ( t.blkcnt - t.rowcnt /
                    floor((p.value - 66 - t.initrans * 24) / greatest(t.avgrln + 2, 11))
                    ) / m.value
                )  reads_wasted
                from
                sys.tab$  t,
                ( select
                    value
                    from
                    sys.v_$parameter
                    where
                    name = 'db_file_multiblock_read_count'
                )  m,
                sys.obj$  o,
                sys.user$  u,
                (select value from sys.v_$parameter where name = 'db_block_size')  p
                where
                t.tab# is null and
                t.blkcnt > m.value and
                t.chncnt = 0 and
                t.avgspc > t.avgrln and
                ceil(
                    ( t.blkcnt - t.rowcnt /
                    floor((p.value - 66 - t.initrans * 24) / greatest(t.avgrln + 2, 11))
                    ) / m.value
                ) > 0 and
                o.obj# = t.obj# and
                o.owner# != 0 and
                u.user# = o.owner#
                order by
                1,5 desc, 2"""
    engine = st.session_state.connections_defined[target]
    return pd.read_sql(sql, engine)

@lowdfcol
def query_perf_load(target, bdate, edate, is_cdb):
    if is_cdb:
        sql = '''with sess_count_grpby_sec as (
                    select con_id,to_char(sample_time,'YYYY-MM-DD HH24:MI:SS') sample_time_str, count(*) sess_count 
                    from gv$active_session_history  
                    where 1 = 1
                    and sample_time >= :bdate
                    and sample_time <= :edate
                    group by con_id, to_char(sample_time,'YYYY-MM-DD HH24:MI:SS')
                    union all
                    select con_id, to_char(sample_time,'YYYY-MM-DD HH24:MI:SS') sample_time_str, count(*) sess_count 
                    from cdb_hist_active_sess_history 
                    where 1 = 1
                    and sample_time >= :bdate
                    and sample_time <= :edate
                    and dbid = (select dbid from v$database)
                    group by con_id, to_char(sample_time,'YYYY-MM-DD HH24:MI:SS')
                    )
                    select con_id, substr(sample_time_str,1,15)||'0' ten_min_timestr, max(sess_count) sess_count
                    from sess_count_grpby_sec
                    group by con_id, substr(sample_time_str,1,15)||'0' 
                    order by con_id, substr(sample_time_str,1,15)||'0' '''
    else:
        sql = '''with sess_count_grpby_sec as (
                select to_char(sample_time,'YYYY-MM-DD HH24:MI:SS') sample_time_str, count(*) sess_count 
                from gv$active_session_history  
                where 1 = 1
                and sample_time >= :bdate
                and sample_time <= :edate
                group by to_char(sample_time,'YYYY-MM-DD HH24:MI:SS')
                union all
                select to_char(sample_time,'YYYY-MM-DD HH24:MI:SS') sample_time_str, count(*) sess_count 
                from dba_hist_active_sess_history 
                where 1 = 1
                and sample_time >= :bdate
                and sample_time <= :edate
                and dbid = (select dbid from v$database)
                group by to_char(sample_time,'YYYY-MM-DD HH24:MI:SS')
                )
                select substr(sample_time_str,1,15)||'0' ten_min_timestr, max(sess_count) sess_count
                from sess_count_grpby_sec
                group by  substr(sample_time_str,1,15)||'0' 
                order by  substr(sample_time_str,1,15)||'0' '''
    engine = st.session_state.connections_defined[target]
    df = pd.read_sql_query(sql, engine, params=[bdate, edate, bdate, edate])
    return df

@lowdfcol
def query_ash_timemodel(target, bdate, edate, is_cdb):
    if is_cdb:
        sql = """select con_id,to_char(trunc((sample_time),'HH'),'MM-DD HH24:MI') TM, state, count(*)/360 sess_cnt
                    from
                    (select  con_id, sample_time,   sample_id       
                    ,  CASE  WHEN session_state = 'ON CPU' THEN 'CPU'       
                            WHEN session_state = 'WAITING' AND wait_class IN ('User I/O') THEN 'IO'
                            WHEN session_state = 'WAITING' AND wait_class IN ('Cluster') THEN 'CLUSTER'
                            ELSE 'WAIT' END state            
                        from CDB_HIST_ACTIVE_SESS_HISTORY             
                        where   session_type IN ( 'FOREGROUND') 
                        and sample_time  between :bdate and :edate  )
                    group by con_id, trunc((sample_time),'HH'), state order by trunc((sample_time),'HH')
                    """
    else:
        sql = """select to_char(trunc((sample_time),'HH'),'MM-DD HH24:MI') TM, state, count(*)/360 sess_cnt
                from
                (select  sample_time,   sample_id       
                ,  CASE  WHEN session_state = 'ON CPU' THEN 'CPU'       
                        WHEN session_state = 'WAITING' AND wait_class IN ('User I/O') THEN 'IO'
                        WHEN session_state = 'WAITING' AND wait_class IN ('Cluster') THEN 'CLUSTER'
                        ELSE 'WAIT' END state            
                    from DBA_HIST_ACTIVE_SESS_HISTORY             
                    where   session_type IN ( 'FOREGROUND') 
                    and sample_time  between :bdate and :edate  )
                group by  trunc((sample_time),'HH'), state order by trunc((sample_time),'HH')
                """
    engine = st.session_state.connections_defined[target]
    df = pd.read_sql_query(sql, engine, params=[bdate, edate])
    return df

@lowdfcol
@functools.lru_cache()
def query_quest_memory(target):
    sql = '''SELECT   a.memory_component, a.memory_size, a.parent_memory_component,
         a.parent_group, a.second_group, a.hasadvisory, a.isfixedsize,
         a.mc_type, a.factor_name, a.parameter, a.default_order,
         CASE
            WHEN a.hasadvisory = 'Y'
            AND a.memory_size = 0
            AND b.collection_status IS NULL
               THEN 'B'
            WHEN a.hasadvisory = 'Y'
            AND a.memory_size <> 0
            AND b.collection_status = 'N'
               THEN 'N'
            WHEN a.hasadvisory = 'Y'
               THEN 'Y'
            ELSE NULL
         END collection_status,
         CASE
            WHEN a.hasadvisory = 'Y'
               THEN CASE
            WHEN a.memory_component = 'Shared Pool'
               THEN a.memory_size
            WHEN a.memory_component = 'Java Pool'
               THEN a.memory_size
            WHEN a.memory_component LIKE '%Default%'
               THEN (SELECT DISTINCT   granule_size/(1024 * 1024)
                                                   granule_size
                                FROM v$sga_dynamic_components)
            WHEN a.memory_component = 'PGA Aggregate Target'
               THEN 10
            ELSE 0
         END
         END minimum_size,
         CASE
            WHEN a.hasadvisory = 'Y'
               THEN CASE
            WHEN a.memory_component = 'PGA Aggregate Target'
               THEN (4096 * 1024) - 1
            ELSE (10 * 1024) - 1
         END
         END maximum_size,
         CASE
            WHEN (SELECT VALUE
                    FROM v$parameter
                   WHERE NAME = 'sga_target') > 0
               THEN CASE
            WHEN a.memory_component = 'Shared Pool'
               THEN 'A'
            WHEN a.memory_component = 'Java Pool'
               THEN 'A'
            WHEN a.memory_component = 'Large Pool'
               THEN 'A'
            WHEN a.memory_component LIKE '%Default%'
               THEN 'A'
            ELSE 'M'
         END
            ELSE 'M'
         END sgamode
        FROM (SELECT memory_component, memory_size, parent_memory_component,
                    CASE
                        WHEN parent_memory_component IS NULL
                        THEN CASE
                        WHEN memory_component = 'Fixed'
                        THEN '1'
                        WHEN memory_component = 'Variable'
                        THEN '2'
                        WHEN memory_component = 'Database Buffers'
                        THEN '3'
                        WHEN memory_component = 'Redo Buffers'
                        THEN '4'
                        WHEN memory_component = 'PGA Aggregate Target'
                        THEN '5'
                    END
                        WHEN parent_memory_component = 'Variable'
                        THEN '2'
                        WHEN parent_memory_component = 'Database Buffers'
                        THEN '3'
                    END parent_group,
                    CASE
                        WHEN memory_component LIKE '% 2k%'
                        THEN '1'
                        WHEN memory_component LIKE '%4k%'
                        THEN '2'
                        WHEN memory_component LIKE '%8k%'
                        THEN '3'
                        WHEN memory_component LIKE '%16k%'
                        THEN '4'
                        WHEN memory_component LIKE '%32k%'
                        THEN '5'
                        WHEN memory_component LIKE '%Keep%'
                        THEN '6'
                        WHEN memory_component LIKE '%Recycle%'
                        THEN '7'
                        WHEN memory_component LIKE '%Shared%'
                        THEN '1'
                        WHEN memory_component LIKE '%Large%'
                        THEN '2'
                        WHEN memory_component LIKE '%Java%'
                        THEN '3'
                        WHEN memory_component LIKE '%Streams%'
                        THEN '4'
                        WHEN memory_component LIKE '%Other%'
                        THEN '5'
                        WHEN memory_component LIKE '%Free%'
                        THEN '6'
                        WHEN memory_component = 'Database Buffers'
                        THEN '0'
                        WHEN memory_component = 'Variable'
                        THEN '0'
                    END second_group,
                    CASE
                        WHEN memory_component LIKE '% 2k%'
                        THEN 'Y'
                        WHEN memory_component LIKE '%4k%'
                        THEN 'Y'
                        WHEN memory_component LIKE '%8k%'
                        THEN 'Y'
                        WHEN memory_component LIKE '%16k%'
                        THEN 'Y'
                        WHEN memory_component LIKE '%32k%'
                        THEN 'Y'
                        WHEN memory_component LIKE '%Keep%'
                        THEN 'Y'
                        WHEN memory_component LIKE '%Recycle%'
                        THEN 'Y'
                        WHEN memory_component LIKE '%Shared%'
                        THEN 'Y'
                        WHEN memory_component LIKE '%PGA%'
                        THEN 'Y'
                        WHEN memory_component LIKE '%Java%'
                        THEN 'Y'
                        --WHEN memory_component LIKE '%Streams%'
                        --   THEN 'Y'
                    ELSE 'N'
                    END hasadvisory,
                    CASE
                        WHEN memory_component LIKE 'Fixed%'
                        THEN 'Y'
                        WHEN memory_component LIKE 'Variable:%'
                        THEN 'Y'
                        WHEN memory_component LIKE 'Redo%'
                        THEN 'Y'
                        ELSE 'N'
                    END isfixedsize,
                    CASE
                        WHEN memory_component LIKE 'PGA%'
                        THEN 'PGA'
                        ELSE 'SGA'
                    END mc_type,
                    CASE
                        WHEN memory_component LIKE '% 2k%'
                        THEN 'Relative Change in physical reads'
                        WHEN memory_component LIKE '%4k%'
                        THEN 'Relative Change in physical reads'
                        WHEN memory_component LIKE '%8k%'
                        THEN 'Relative Change in physical reads'
                        WHEN memory_component LIKE '%16k%'
                        THEN 'Relative Change in physical reads'
                        WHEN memory_component LIKE '%32k%'
                        THEN 'Relative Change in physical reads'
                        WHEN memory_component LIKE '%Keep%'
                        THEN 'Relative Change in physical reads'
                        WHEN memory_component LIKE '%Recycle%'
                        THEN 'Relative Change in physical reads'
                        WHEN memory_component LIKE '%Shared%'
                        THEN 'Relative change in parse time loadings'
                        WHEN memory_component LIKE '%Java%'
                        THEN 'Relative change in parse time loadings'
                        WHEN memory_component LIKE '%PGA%'
                        THEN 'Cache hit percentage'
                        --WHEN memory_component LIKE '%Streams%'
                        --   THEN 'Dequeue rate'
                    ELSE NULL
                    END factor_name,
                    CASE
                        WHEN memory_component LIKE '% 2k%'
                        AND memory_component LIKE '%Default%'
                        THEN 'db_cache_size'
                        WHEN memory_component LIKE '%4k%'
                        AND memory_component LIKE '%Default%'
                        THEN 'db_cache_size'
                        WHEN memory_component LIKE '%8k%'
                        AND memory_component LIKE '%Default%'
                        THEN 'db_cache_size'
                        WHEN memory_component LIKE '%16k%'
                        AND memory_component LIKE '%Default%'
                        THEN 'db_cache_size'
                        WHEN memory_component LIKE '%32k%'
                        AND memory_component LIKE '%Default%'
                        THEN 'db_cache_size'
                        WHEN memory_component LIKE '% 2k%'
                        AND memory_component NOT LIKE '%Default%'
                        THEN 'db_2k_cache_size'
                        WHEN memory_component LIKE '%4k%'
                        AND memory_component NOT LIKE '%Default%'
                        THEN 'db_4k_cache_size'
                        WHEN memory_component LIKE '%8k%'
                        AND memory_component NOT LIKE '%Default%'
                        THEN 'db_8k_cache_size'
                        WHEN memory_component LIKE '%16k%'
                        AND memory_component NOT LIKE '%Default%'
                        THEN 'db_16k_cache_size'
                        WHEN memory_component LIKE '%32k%'
                        AND memory_component NOT LIKE '%Default%'
                        THEN 'db_32k_cache_size'
                        WHEN memory_component LIKE '%Keep%'
                        THEN 'db_keep_cache_size'
                        WHEN memory_component LIKE '%Recycle%'
                        THEN 'db_recycle_cache_size'
                        WHEN memory_component LIKE '%Shared%'
                        THEN 'shared_pool_size'
                        WHEN memory_component LIKE '%PGA%'
                        THEN 'pga_aggregate_target'
                        WHEN memory_component LIKE '%Java%'
                        THEN 'java_pool_size'
                        WHEN memory_component LIKE '%Large%'
                        THEN 'large_pool_size'
                        WHEN memory_component LIKE '%Streams%'
                        THEN 'streams_pool_size'
                        ELSE NULL
                    END parameter,
                    CASE
                        WHEN memory_component LIKE '% 2k%'
                        AND memory_component LIKE '%Default%'
                        THEN 2
                        WHEN memory_component LIKE '%4k%'
                        AND memory_component LIKE '%Default%'
                        THEN 2
                        WHEN memory_component LIKE '%8k%'
                        AND memory_component LIKE '%Default%'
                        THEN 2
                        WHEN memory_component LIKE '%16k%'
                        AND memory_component LIKE '%Default%'
                        THEN 2
                        WHEN memory_component LIKE '%32k%'
                        AND memory_component LIKE '%Default%'
                        THEN 2
                        WHEN memory_component LIKE '% 2k%'
                        AND memory_component NOT LIKE '%Default%'
                        THEN 6
                        WHEN memory_component LIKE '%4k%'
                        AND memory_component NOT LIKE '%Default%'
                        THEN 7
                        WHEN memory_component LIKE '%8k%'
                        AND memory_component NOT LIKE '%Default%'
                        THEN 8
                        WHEN memory_component LIKE '%16k%'
                        AND memory_component NOT LIKE '%Default%'
                        THEN 9
                        WHEN memory_component LIKE '%32k%'
                        AND memory_component NOT LIKE '%Default%'
                        THEN 10
                        WHEN memory_component LIKE '%Keep%'
                        THEN 4
                        WHEN memory_component LIKE '%Recycle%'
                        THEN 5
                        WHEN memory_component LIKE '%Shared%'
                        THEN 1
                        WHEN memory_component LIKE '%PGA%'
                        THEN 3
                        WHEN memory_component LIKE '%Java%'
                        THEN 11
                        --WHEN memory_component LIKE '%Streams%'
                        --   THEN 12
                    ELSE NULL
                    END default_order
                FROM (SELECT REPLACE (NAME, ' Size') memory_component,
                            ROUND (VALUE / 1048576, 2) memory_size,
                            NULL parent_memory_component
                        FROM v$sga
                    UNION
                    SELECT DECODE
                                (component,
                                'DEFAULT buffer cache', (SELECT    'DB Cache (Default '
                                                                || VALUE / 1024
                                                                || 'k)'
                                                            FROM v$parameter
                                                        WHERE NAME =
                                                                    'db_block_size'),
                                DECODE
                                    (INSTR (component, 'cache'),
                                    0, INITCAP (component),
                                        'DB '
                                    || INITCAP
                                        (REPLACE
                                            (REPLACE (REPLACE (component,
                                                                'DEFAULT '
                                                                ),
                                                        'blocksize buffer cache',
                                                        'cache'
                                                        ),
                                                'buffer cache',
                                                'cache'
                                            )
                                        )
                                    )
                                ) memory_component,
                            TO_NUMBER (current_size) / 1048576 memory_size,
                            DECODE (INSTR (component, 'cache'),
                                    0, 'Variable',
                                    'Database Buffers'
                                    ) parent_memory_component
                        FROM v$sga_dynamic_components
                    WHERE component <> 'OSM Buffer Cache'
                        AND NVL (SUBSTR (component,
                                        INSTR (component, ' ') + 1,
                                            INSTR (component, 'K')
                                        - INSTR (component, ' ')
                                        ),
                                'XX'
                                ) <> (SELECT VALUE / 1024 || 'K'
                                        FROM v$parameter
                                        WHERE NAME = 'db_block_size')
                    UNION
                    SELECT 'PGA Aggregate Target', VALUE / (1024 * 1024), NULL
                        FROM v$parameter
                    WHERE NAME = 'pga_aggregate_target'
                    UNION
                    SELECT 'Variable: Others' memory_component,
                            ROUND ((c.VALUE - d.current_size - e.VALUE) / 1048576,
                                    2
                                ) memory_size,
                            'Variable' parent_memory_component
                        FROM (SELECT NAME, VALUE
                                FROM v$sga
                            WHERE NAME = 'Variable Size') c,
                            (SELECT current_size
                                FROM v$sga_dynamic_free_memory) d,
                            (SELECT SUM (current_size) VALUE
                                FROM v$sga_dynamic_components
                            WHERE component IN
                                        ('shared pool',
                                        'large pool',
                                        'java pool',
                                        'streams pool'
                                        )) e
                    UNION
                    SELECT 'Free' memory_component,
                            TO_NUMBER (current_size) / 1048576 memory_size,
                            'Variable' parent_memory_component
                        FROM v$sga_dynamic_free_memory)) a,
            (SELECT a.memory_component,
                    NVL (b.collection_status,
                        a.collection_status
                        ) collection_status
                FROM (SELECT CASE
                                WHEN NAME = 'DEFAULT'
                                AND b.bsize = block_size
                                THEN    'DB Cache (Default '
                                        || block_size / 1024
                                        || 'k)'
                                WHEN NAME <> 'DEFAULT'
                                THEN 'DB ' || INITCAP (NAME) || ' Cache'
                                ELSE 'DB ' || block_size / 1024 || 'k Cache'
                            END memory_component,
                            'Y' collection_status
                        FROM v$db_cache_advice a,
                            (SELECT VALUE bsize
                                FROM v$parameter
                            WHERE NAME = 'db_block_size') b
                    WHERE size_factor = 1) a,
                    (SELECT CASE
                                WHEN NAME = 'DEFAULT'
                                AND b.bsize = block_size
                                THEN    'DB Cache (Default '
                                        || block_size / 1024
                                        || 'k)'
                                WHEN NAME <> 'DEFAULT'
                                THEN 'DB ' || INITCAP (NAME) || ' Cache'
                                ELSE 'DB ' || block_size / 1024 || 'k Cache'
                            END memory_component,
                            'N' collection_status
                        FROM (SELECT   NAME, block_size
                                FROM v$db_cache_advice m
                                WHERE estd_physical_read_factor = 1
                                    OR estd_physical_read_factor IS NULL
                            GROUP BY NAME, block_size
                                HAVING COUNT (*) =
                                        (SELECT COUNT (*)
                                            FROM v$db_cache_advice
                                            WHERE NAME = m.NAME
                                            AND block_size = m.block_size)) a,
                            (SELECT VALUE bsize
                                FROM v$parameter
                            WHERE NAME = 'db_block_size') b) b
            WHERE a.memory_component = b.memory_component(+)
            UNION
            SELECT a.memory_component,
                    NVL (b.collection_status,
                        a.collection_status
                        ) collection_status
                FROM (SELECT 'Shared Pool' memory_component,
                            'Y' collection_status
                        FROM v$shared_pool_advice
                    WHERE shared_pool_size_factor = 1) a,
                    (SELECT   'Shared Pool' memory_component,
                            'N' collection_status
                        FROM v$shared_pool_advice
                        WHERE estd_lc_load_time_factor = 1
                            OR estd_lc_load_time_factor IS NULL
                    GROUP BY 1
                        HAVING COUNT (*) = (SELECT COUNT (*)
                                            FROM v$shared_pool_advice)) b
            WHERE a.memory_component = b.memory_component(+)
            UNION
            SELECT a.memory_component,
                    NVL (b.collection_status,
                        a.collection_status
                        ) collection_status
                FROM (SELECT 'Java Pool' memory_component, 'Y' collection_status
                        FROM v$java_pool_advice
                    WHERE java_pool_size_factor = 1) a,
                    (SELECT   'Java Pool' memory_component,
                            'N' collection_status
                        FROM v$java_pool_advice
                        WHERE estd_lc_load_time_factor = 1
                            OR estd_lc_load_time_factor IS NULL
                    GROUP BY 1
                        HAVING COUNT (*) = (SELECT COUNT (*)
                                            FROM v$java_pool_advice)) b
            WHERE a.memory_component = b.memory_component(+)
            UNION
            SELECT a.memory_component,
                    NVL (b.collection_status,
                        a.collection_status
                        ) collection_status
                FROM (SELECT 'PGA Aggregate Target' memory_component,
                            'Y' collection_status
                        FROM v$pga_target_advice
                    WHERE pga_target_factor = 1) a,
                    (SELECT   'PGA Aggregate Target' memory_component,
                            'N' collection_status
                        FROM v$pga_target_advice
                        WHERE (   estd_pga_cache_hit_percentage = 100
                                OR estd_pga_cache_hit_percentage IS NULL
                                OR estd_pga_cache_hit_percentage = 0
                            )
                    GROUP BY 1
                        HAVING COUNT (*) = (SELECT COUNT (*)
                                            FROM v$pga_target_advice)) b
            WHERE a.memory_component = b.memory_component(+)) b
    WHERE a.memory_component = b.memory_component(+)
    ORDER BY a.parent_group, a.second_group '''
    engine = st.session_state.connections_defined[target]
    return pd.read_sql(sql, engine)

@lowdfcol
@functools.lru_cache()
def query_autotasks(target, is_cdb):
    if is_cdb:
        sql = '''select t.client_name, t.task_name, t.operation_name, t.status, t.con_id,
                w.window_group_name, w.enabled, w.NEXT_START_DATE
            from cdb_autotask_task t, cdb_autotask_client c, CDB_SCHEDULER_WINDOW_GROUPS w
            where t.con_id = c.con_id and t.client_name = c.client_name
            and c.con_id = w.con_id and c.window_group = w.window_group_name 
        '''
    else:
        sql = '''select t.client_name, t.task_name, t.operation_name, t.status, 
            w.window_group_name, w.enabled, w.NEXT_START_DATE
        from dba_autotask_task t, dba_autotask_client c, DBA_SCHEDULER_WINDOW_GROUPS w
        where  t.client_name = c.client_name
           and c.window_group = w.window_group_name 
    '''
    engine = st.session_state.connections_defined[target]
    return pd.read_sql(sql, engine)

@lowdfcol
@functools.lru_cache()
def query_sched_jobs(target, is_cdb):
    if is_cdb:
        sql = '''select con_id,owner,job_name,program_owner,program_name,job_type,
            start_date,to_char(repeat_interval) repeat_interval,enabled,state,run_count,failure_count,
            last_start_date,extract(day from last_run_duration*86400) last_run_duration,next_run_date
            from cdb_scheduler_jobs '''
    else:
        sql = '''select owner,job_name,program_owner,program_name,job_type,
        start_date,to_char(repeat_interval) repeat_interval,enabled,state,run_count,failure_count,
        last_start_date,extract(day from last_run_duration*86400) last_run_duration,next_run_date
        from dba_scheduler_jobs '''
    engine = st.session_state.connections_defined[target]
    return pd.read_sql(sql, engine)

@lowdfcol
@functools.lru_cache()
def query_event_class(target):
    sql = '''select distinct name, wait_class from v$event_name'''
    engine = st.session_state.connections_defined[target]
    return pd.read_sql(sql, engine)

@lowdfcol
def query_awr_latch(target, bdate, edate, is_cdb):
    if is_cdb:
        sql = '''select * from (select con_id,
                    dbid,
                    snap_id,
                    level#,latch_hash,instance_number,latch_name,
                    begin_interval_time,
                    end_interval_time,
                    wait_time,
                    lead(wait_time, 1, null) over (
                        partition by con_id,dbid,
                        instance_number,
                        latch_name
                        order by
                        snap_id
                    ) - wait_time wait_time_delta,
                    variance(wait_time) over (partition by con_id,dbid, instance_number, latch_name) waits_var
                    from (
                        select
                    latch.con_id,
                    latch.dbid,
                    latch.snap_id,
                    latch.level#,latch.latch_hash,latch.instance_number,latch.latch_name,
                    snap.begin_interval_time,
                    snap.end_interval_time,
                    sum(wait_time) wait_time
                    from cdb_hist_latch latch,
                    cdb_hist_snapshot snap
                    where
                    --latch.con_id = snap.con_id
                    latch.snap_id = snap.snap_id
                    and latch.dbid = snap.dbid
                    and snap.begin_interval_time >= :bdate
                    and snap.end_interval_time <= :edate
                    group by
                    latch.con_id,
                    latch.dbid,
                    latch.snap_id,
                    latch.level#,latch.latch_hash,latch.instance_number,latch.latch_name,
                    snap.begin_interval_time,
                    snap.end_interval_time
                    order by
                    latch.con_id,
                    latch.instance_number,
                    latch.latch_name,
                    latch.snap_id
                    ) T)
                    where
                    1=1
                    and WAITS_VAR != 0
            '''
    else:
        sql = '''select * from (select 
                dbid,
                snap_id,
                level#,latch_hash,instance_number,latch_name,
                begin_interval_time,
                end_interval_time,
                wait_time,
                lead(wait_time, 1, null) over (
                    partition by dbid,
                    instance_number,
                    latch_name
                    order by
                    snap_id
                ) - wait_time wait_time_delta,
                variance(wait_time) over (partition by dbid, instance_number, latch_name) waits_var
                from (
                    select
                latch.dbid,
                latch.snap_id,
                latch.level#,latch.latch_hash,latch.instance_number,latch.latch_name,
                snap.begin_interval_time,
                snap.end_interval_time,
                sum(wait_time) wait_time
                from dba_hist_latch latch,
                dba_hist_snapshot snap
                where
                latch.snap_id = snap.snap_id
                and latch.dbid = snap.dbid
                and snap.begin_interval_time >= :bdate
                and snap.end_interval_time <= :edate
                group by
                latch.dbid,
                latch.snap_id,
                latch.level#,latch.latch_hash,latch.instance_number,latch.latch_name,
                snap.begin_interval_time,
                snap.end_interval_time
                order by
                latch.instance_number,
                latch.latch_name,
                latch.snap_id
                ) T)
                where
                1=1
                and WAITS_VAR != 0
        '''
    engine = st.session_state.connections_defined[target]
    return pd.read_sql_query(sql, engine, params=[bdate, edate])

@lowdfcol
def query_awr_bg_event(target, bdate, edate, is_cdb):
    if is_cdb:
        sql = '''select * from (select con_id,
                    dbid,
                    snap_id,
                    instance_number,event_name,
                    begin_interval_time,
                    end_interval_time,
                    wait_time,
                    lead(wait_time, 1, null) over (
                        partition by con_id,dbid,
                        instance_number,
                        event_name
                        order by
                        snap_id
                    ) - wait_time wait_time_delta,
                    variance(wait_time) over (partition by con_id,dbid, instance_number, event_name) waits_var
                    from (
                        select
                            stats.con_id,
                            stats.dbid,
                            stats.snap_id,
                            stats.instance_number,stats.event_name,
                            snap.begin_interval_time,
                            snap.end_interval_time,
                            sum(TIME_WAITED_MICRO) wait_time
                        from cdb_hist_bg_event_summary stats,
                            cdb_hist_snapshot snap
                        where
                            --latch.con_id = snap.con_id
                            stats.snap_id = snap.snap_id
                            and stats.dbid = snap.dbid
                            and snap.begin_interval_time >= :bdate
                            and snap.end_interval_time <= :edate
                            group by
                            stats.con_id,
                            stats.dbid,
                            stats.snap_id,
                            stats.instance_number,stats.event_name,
                            snap.begin_interval_time,
                            snap.end_interval_time
                            order by
                            stats.con_id,
                            stats.instance_number,
                            stats.event_name,
                            stats.snap_id
                    ) T)
                    where
                    1=1
                    and WAITS_VAR != 0
            '''
    else:
        sql = '''select * from (select 
                dbid,
                snap_id,
                instance_number,event_name,
                begin_interval_time,
                end_interval_time,
                wait_time,
                lead(wait_time, 1, null) over (
                    partition by dbid,
                    instance_number,
                    event_name
                    order by
                    snap_id
                ) - wait_time wait_time_delta,
                variance(wait_time) over (partition by dbid, instance_number, event_name) waits_var
                from (
                    select
                        stats.dbid,
                        stats.snap_id,
                        stats.instance_number,stats.event_name,
                        snap.begin_interval_time,
                        snap.end_interval_time,
                        sum(TIME_WAITED_MICRO) wait_time
                    from dba_hist_bg_event_summary stats,
                        dba_hist_snapshot snap
                    where
                        stats.snap_id = snap.snap_id
                        and stats.dbid = snap.dbid
                        and snap.begin_interval_time >= :bdate
                        and snap.end_interval_time <= :edate
                        group by
                        stats.dbid,
                        stats.snap_id,
                        stats.instance_number,stats.event_name,
                        snap.begin_interval_time,
                        snap.end_interval_time
                        order by
                        stats.instance_number,
                        stats.event_name,
                        stats.snap_id
                ) T)
                where
                1=1
                and WAITS_VAR != 0
        '''
    engine = st.session_state.connections_defined[target]
    return pd.read_sql_query(sql, engine, params=[bdate, edate])

@lowdfcol
@functools.lru_cache()
def query_awr_generic(target, bdate, edate, awr_table, stat_name_column, stat_value_column, is_cdb):
    if is_cdb:
        sql = '''select * from (select con_id,
                    dbid,
                    snap_id,
                    instance_number,{stat_name_column} stat_name,
                    begin_interval_time,
                    end_interval_time,
                    wait_time,
                    lead(wait_time, 1, null) over (
                        partition by con_id,dbid,
                        instance_number,
                        {stat_name_column}
                        order by
                        snap_id
                    ) - wait_time wait_time_delta,
                    variance(wait_time) over (partition by con_id,dbid, instance_number, {stat_name_column}) waits_var
                    from (
                        select
                            stats.con_id,
                            stats.dbid,
                            stats.snap_id,
                            stats.instance_number,stats.{stat_name_column},
                            snap.begin_interval_time,
                            snap.end_interval_time,
                            sum({stat_value_column}) wait_time
                        from {awr_table} stats,
                            cdb_hist_snapshot snap
                        where
                            --latch.con_id = snap.con_id
                            stats.snap_id = snap.snap_id
                            and stats.dbid = snap.dbid
                            and snap.begin_interval_time >= :bdate
                            and snap.end_interval_time <= :edate
                            group by
                            stats.con_id,
                            stats.dbid,
                            stats.snap_id,
                            stats.instance_number,stats.{stat_name_column},
                            snap.begin_interval_time,
                            snap.end_interval_time
                            order by
                            stats.con_id,
                            stats.instance_number,
                            stats.{stat_name_column},
                            stats.snap_id
                    ) T)
                    where
                    1=1
                    and WAITS_VAR != 0
            '''.format(awr_table=awr_table, stat_name_column=stat_name_column, stat_value_column= stat_value_column)
    else:
        sql = '''select * from (select 
                dbid,
                snap_id,
                instance_number,{stat_name_column} stat_name,
                begin_interval_time,
                end_interval_time,
                wait_time,
                lead(wait_time, 1, null) over (
                    partition by dbid,
                    instance_number,
                    {stat_name_column}
                    order by
                    snap_id
                ) - wait_time wait_time_delta,
                variance(wait_time) over (partition by dbid, instance_number, {stat_name_column}) waits_var
                from (
                    select
                        stats.dbid,
                        stats.snap_id,
                        stats.instance_number,stats.{stat_name_column},
                        snap.begin_interval_time,
                        snap.end_interval_time,
                        sum({stat_value_column}) wait_time
                    from {awr_table} stats,
                        dba_hist_snapshot snap
                    where
                        stats.snap_id = snap.snap_id
                        and stats.dbid = snap.dbid
                        and snap.begin_interval_time >= :bdate
                        and snap.end_interval_time <= :edate
                        group by
                        stats.dbid,
                        stats.snap_id,
                        stats.instance_number,stats.{stat_name_column},
                        snap.begin_interval_time,
                        snap.end_interval_time
                        order by
                        stats.instance_number,
                        stats.{stat_name_column},
                        stats.snap_id
                ) T)
                where
                1=1
                and WAITS_VAR != 0
        '''.format(awr_table=awr_table, stat_name_column=stat_name_column, stat_value_column=stat_value_column)
    engine = st.session_state.connections_defined[target]
    return pd.read_sql_query(sql, engine, params=[bdate, edate])

@lowdfcol
def query_awr_time_model(target, bdate, edate, is_cdb):
    if is_cdb:
        sql = '''select con_id,dbid,snap_id,instance_number,stat_name,begin_interval_time,end_interval_time,
                wait_time,wait_time_delta, round(waits_var,2) waits_var from (select con_id,
                    dbid,
                    snap_id,
                    instance_number,stat_name,
                    begin_interval_time,
                    end_interval_time,
                    wait_time,
                    lead(wait_time, 1, null) over (
                        partition by con_id,dbid,
                        instance_number,
                        stat_name
                        order by
                        snap_id
                    ) - wait_time wait_time_delta,
                    variance(wait_time) over (partition by con_id,dbid, instance_number, stat_name) waits_var
                    from (
                        select
                            stats.con_id,
                            stats.dbid,
                            stats.snap_id,
                            stats.instance_number,stats.stat_name,
                            snap.begin_interval_time,
                            snap.end_interval_time,
                            sum(value) wait_time
                        from cdb_hist_sys_time_model stats,
                            cdb_hist_snapshot snap
                        where
                            --latch.con_id = snap.con_id
                            stats.snap_id = snap.snap_id
                            and stats.dbid = snap.dbid
                            and snap.begin_interval_time >= :bdate
                            and snap.end_interval_time <= :edate
                            group by
                            stats.con_id,
                            stats.dbid,
                            stats.snap_id,
                            stats.instance_number,stats.stat_name,
                            snap.begin_interval_time,
                            snap.end_interval_time
                            order by
                            stats.con_id,
                            stats.instance_number,
                            stats.stat_name,
                            stats.snap_id
                    ) T)
                    where
                    1=1
                    and WAITS_VAR != 0
            '''
    else:
        sql = '''select dbid,snap_id,instance_number,stat_name,begin_interval_time,end_interval_time,
            wait_time,wait_time_delta, round(waits_var,2) waits_var from (select 
                dbid,
                snap_id,
                instance_number,stat_name,
                begin_interval_time,
                end_interval_time,
                wait_time,
                lead(wait_time, 1, null) over (
                    partition by dbid,
                    instance_number,
                    stat_name
                    order by
                    snap_id
                ) - wait_time wait_time_delta,
                variance(wait_time) over (partition by dbid, instance_number, stat_name) waits_var
                from (
                    select
                        stats.dbid,
                        stats.snap_id,
                        stats.instance_number,stats.stat_name,
                        snap.begin_interval_time,
                        snap.end_interval_time,
                        sum(value) wait_time
                    from dba_hist_sys_time_model stats,
                        dba_hist_snapshot snap
                    where
                        stats.snap_id = snap.snap_id
                        and stats.dbid = snap.dbid
                        and snap.begin_interval_time >= :bdate
                        and snap.end_interval_time <= :edate
                        group by
                        stats.dbid,
                        stats.snap_id,
                        stats.instance_number,stats.stat_name,
                        snap.begin_interval_time,
                        snap.end_interval_time
                        order by
                        stats.instance_number,
                        stats.stat_name,
                        stats.snap_id
                ) T)
                where
                1=1
                and WAITS_VAR != 0
        '''
    engine = st.session_state.connections_defined[target]
    return pd.read_sql_query(sql, engine, params=[bdate, edate])



def layer_df(data:pd.DataFrame, parent_column, id_column,
             name_label, value_label, name_property, value_property, store_dict=[],  
             current_value=None):
    '''
    Description: 将dataframe转换为hierarchy结构，为注入echart sunburst图提供数据
    Params:
        data: dataframe
        parent_column: parent 列名
        id_column: id 列名
        name_label: 名称属性 echart sunburst中默认为name
        value_label: 值属性 echart sunburst中默认为value
        name_property: 名称对应的dataframe列
        value_property: 值对应的dataframe列
        store_dict: 保存结果
        current_value: 当前id，根据此id 获取所有children
    '''
    if current_value is None:
        df_children: pd.DataFrame = data[data[parent_column].isna()]
    else:
        df_children: pd.DataFrame = data[
            data[parent_column] == current_value]
    # 将 df_current 插入 store_dict
    if df_children.shape[0] == 0:
        return
    else:
        for _, item in df_children.iterrows():
            t_obj = {name_label: item[name_property],
                     value_label: item[value_property], 'children': []}
            store_dict.append(t_obj)
            layer_df(data, parent_column, id_column, name_label, value_label, name_property, value_property,
                     t_obj['children'], item[id_column])

@lowdfcol
def query_uncommit_tx(target, is_cdb):
    if is_cdb:
        sql = """select s.con_id,s.inst_id, s.sid, start_time, username, r.name undo_name,  
            ubafil, ubablk, t.status, s.status sess_status,(used_ublk*p.value)/1024 blk, used_urec
            from gv$transaction t, v$rollname r, gv$session s, v$parameter p
            where xidusn=usn
            and s.saddr=t.ses_addr
            and p.name='db_block_size'
            order by 1"""
    else:
        sql = """select s.inst_id, s.sid, start_time, username, r.name undo_name,  
            ubafil, ubablk, t.status, s.status sess_status,(used_ublk*p.value)/1024 blk, used_urec
            from gv$transaction t, v$rollname r, gv$session s, v$parameter p
            where xidusn=usn
            and s.saddr=t.ses_addr
            and p.name='db_block_size'
            order by 1"""
    engine = st.session_state.connections_defined[target]
    return pd.read_sql(sql, engine)


# col UNXPSTEALCNT format 999, 999, 999  heading "# Unexpired|Stolen"
# col EXPSTEALCNT format 999, 999, 999   heading "# Expired|Reused"
# col SSOLDERRCNT format 999, 999, 999   heading "ORA-1555|Error"
# col NOSPACEERRCNT format 999, 999, 999 heading "Out-Of-space|Error"
# col MAXQUERYLEN format 999, 999, 999   heading "Max Query|Length"
# col TUNED_UNDORETENTION format 999, 999, 999  heading "Auto-Ajusted|Undo Retention"
# col hours format 999, 999 heading "Tuned|(HRs)"
@lowdfcol
def query_undo_stat(target):
    sql = """select inst_id, to_char(begin_time,'MM/DD/YYYY HH24:MI') begin_time, 
                UNXPSTEALCNT, EXPSTEALCNT , SSOLDERRCNT, NOSPACEERRCNT, MAXQUERYLEN,
                TUNED_UNDORETENTION, TUNED_UNDORETENTION/60/60 hours
            from gv$undostat
            where begin_time between (sysdate-.16) 
                                and sysdate
            order by inst_id, begin_time
            """
    engine = st.session_state.connections_defined[target]
    return pd.read_sql(sql, engine)
