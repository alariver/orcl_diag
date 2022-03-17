import streamlit as st
import time
import pandas as pd
import numpy as np
import cx_Oracle
import sqlalchemy
import functools
import datetime

from pyecharts.charts import Line
import pyecharts.options as opts
from pyecharts.globals import ThemeType
from streamlit_echarts import st_pyecharts
from pyecharts.faker import Faker
# import matplotlib.pyplot as plt
# import altair as alt
from streamlit_echarts import st_echarts

from io import BytesIO, StringIO
import tarfile

from queryUtil import *

st.set_page_config(layout="wide")

if 'connections_defined' not in st.session_state:
    st.session_state.connections_defined = {}

# 如果上载了压缩文件，则运行于数据展示模式
file_uploaded = False

gen_tar_data: BytesIO = BytesIO()
tar_file = tarfile.open(mode="w:gz", fileobj=gen_tar_data)

# 将dataframe以csv格式保存在tarfile中
def df2tar(df:pd.DataFrame, tar:tarfile.TarFile, name:str):
    out = StringIO()
    df.to_csv(out, sep=",", index=False)
    info = tarfile.TarInfo(name=name)
    bytes_out = out.getvalue().encode('utf-8')
    info.size = len(bytes_out)
    tar.addfile(info, fileobj=BytesIO(bytes_out))
    out.close()

import io

# 根据名称，从tarfile中的csv文件，读取dataframe
def df_from_tar(name:str):
    print('df_from_tar, ', name)
    # x:io.BufferedReader = None
    # x.read()
    if not name.endswith('.csv'):
        name += '.csv'
    if tar_file:
        if name in tar_file.getnames():
            return pd.read_csv(BytesIO((tar_file.extractfile(name)).read()))
        else:
            return None
    else:
        st.error('上载的文件包有误。')
        st.stop()

def filter_pdb(df:pd.DataFrame, column_name:str):
    if st.session_state.selected_pdb != 'All':
        _s = df[column_name] == st.session_state.selected_pdb
    else:
        _s = df[column_name].apply(lambda x:  True)
    return _s

def add_connection():
    file_uploaded = False
    if st.session_state.host and st.session_state.port and st.session_state.service and st.session_state.username and st.session_state.password:
        conn_str = f'oracle+cx_oracle://{st.session_state.username}:{st.session_state.password}@{st.session_state.host}:{st.session_state.port}/?service_name={st.session_state.service}'
        key_str = f'{st.session_state.username}@{st.session_state.host}/{st.session_state.service}'
        engine = sqlalchemy.create_engine(conn_str, arraysize=1000)
        if key_str not in st.session_state.connections_defined:

            st.info('连接信息已经添加！')
        else:
            st.warning('连接信息已存在, 覆盖原连接配置。')
            st.session_state.connections_defined[key_str].dispose()
        st.session_state.connections_defined[key_str] = engine
    else:
        st.warning('请填写完整信息.')

def util_gen_awr_section(section_name,section_anchor, table_name, 
        stat_column, value_column, is_cdb,df_pdb, include_stats:list=None, unit='ms', exclude_stats:list=None):
    st.subheader(section_name, anchor=section_anchor)
    df:pd.DataFrame = query_awr_generic(st.session_state.selected_conn, datetime.datetime.now(
    ) - datetime.timedelta(1), datetime.datetime.now(), table_name, stat_column, value_column, is_cdb) if not file_uploaded else df_from_tar(section_anchor)
    if df is not None:
        if not file_uploaded:
            df2tar(df, tar_file, f'{section_anchor}.csv')
        df = df.fillna(method='ffill')
        # df
        if is_cdb:
            df = df.merge(df_pdb[['con_id', 'name']], on='con_id')
            _s = filter_pdb(df, 'name')
            df = df.loc[_s, ['name', 'instance_number', 'stat_name', 'begin_interval_time', 'wait_time', 'wait_time_delta', 'waits_var'
                            ]]
        else:
            df = df[['name', 'instance_number', 'stat_name', 'begin_interval_time', 'wait_time', 'wait_time_delta', 'waits_var'
                    ]]
        df['begin_interval_time'] = pd.to_datetime(df['begin_interval_time'])
        df['begin_interval_time'] = df['begin_interval_time'].apply(
            lambda x: datetime.datetime.strftime(x, '%m-%d %H:%M'))
        if st.checkbox(f'显示AWR {section_name} 原始数据'):
            st.write(df.rename(columns=translate_word))
        # latch chart， 每个container一个单独的chart， instance_number+latch_name 决定series， begin_interval_date 作为x轴
        for con_name in df['name'].unique():
            # st.title(con_name + ' 历史Latch等待， 数据来源AWR')
            df_con:pd.DataFrame = df.loc[df['name'] == con_name]
            df_con['waits_var']=df_con['waits_var'].astype(float)
            
            # Top-N gets
            var_ser = df_con.groupby(['instance_number', 'stat_name'])[
                'waits_var'].mean().sort_values().tail(n=15)
            var_ser = var_ser.reset_index()
            top_n_stats:list = (var_ser['stat_name']+'@'+var_ser['instance_number'].astype(str)).tolist()
            if include_stats:
                for inst_num in df_con['instance_number'].unique():
                    top_n_stats = top_n_stats+[x+'@'+str(inst_num) for x in include_stats]
            
            not_in_stats = []
            if exclude_stats:
                for inst_num in df_con['instance_number'].unique():
                    not_in_stats = not_in_stats+[x+'@'+str(inst_num) for x in exclude_stats]
            date_index = df_con['begin_interval_time'].sort_values(
            ).unique().tolist()
            # date_index
            df_con['stat_name'] = df_con['stat_name'] +'@'+ df_con['instance_number'].astype(str)
            df_con = df_con[['begin_interval_time','stat_name','wait_time_delta']]
            df_con = df_con.pivot_table(index='begin_interval_time', columns='stat_name', 
                values='wait_time_delta', fill_value=0)
            df_con.reindex(date_index)
            
            option = {
                'title': {
                    'text': con_name + f' {section_name} (AWR快照间隔内[默认1小时],单位 {unit})， 数据来源AWR'
                },
                'tooltip': {
                    'trigger': 'axis'
                },
                'legend': {
                    'bottom': 'bottom'
                },
                'grid': {
                    'left': '3%',
                    'right': '4%',
                    'bottom': '20%',
                    'containLabel': True
                },
                'toolbox': {
                    'feature': {
                        'saveAsImage': {}
                    }
                },
                'xAxis': {
                    'type': 'category',
                    'data': df_con.index.tolist()
                },
                'yAxis': {
                    'type': 'value'
                },
                'series': [
                    {
                        'name': col,
                        'data': df_con[col].values.tolist(), #grp_data['wait_time_delta'].fillna(method='ffill').values.tolist(),
                        'type': 'line',
                        'smooth': True
                    } for col in df_con if col in top_n_stats and col not in not_in_stats
                    #for (_instance_number, _event_name), grp_data in df_con.groupby(['instance_number', 'stat_name']) if _event_name in top_n_stats

                ]
            }
            st_echarts(option, height=600)
    else:
        st.warning('数据未存储')
    return df

def render_topsql_section(df:pd.DataFrame, is_cdb:bool, df_pdb:pd.DataFrame):
    if df is not None:
        if is_cdb:
            df = df.merge(df_pdb[['con_id', 'name']], on='con_id')
            _s = filter_pdb(df, 'name')

            df = df.loc[_s, ['name', 'sql_id', 'value', 'sql_text',]]
        else:
            df = df[['sql_id', 'value', 'sql_text']]

        st.write(df.rename(columns=translate_word))
    else:
        st.warning('数据未存储')

## sidebar
with st.sidebar.expander(label="添加链接..."):

    st.text_input(label='Host', key="host")
    st.text_input(label='Port', value="1521", key="port")
    st.text_input(label='Service', key="service")
    st.text_input(label='Username', key="username")
    st.text_input(label='Password', key="password", type="password")
    add_connection = st.button(label="添加连接", on_click=add_connection)

st.sidebar.selectbox(
    label='选择连接', options=st.session_state.connections_defined.keys(), key='selected_conn')

pdb_choose_container = st.sidebar.empty()

show_storage_file = st.sidebar.checkbox(label='显示数据库存储、文件信息')

show_sql_info = st.sidebar.checkbox(label='显示SQL、Top SQL信息')

show_io_statistics = st.sidebar.checkbox(label='显示文件IO信息')

show_current_sessions = st.sidebar.checkbox(label='显示当前Session信息')

show_awr_loads = st.sidebar.checkbox(label='显示系统历史负载')

show_jobs = st.sidebar.checkbox(label='显示JOB信息')

# 上传文件处理
uploaded_file = st.sidebar.file_uploader("上传、显示离线数据包...")
if uploaded_file is not None:
    # To read file as bytes:
    print('upload file...', uploaded_file.name)
    bytes_data = BytesIO(uploaded_file.getvalue())
    tar_file = tarfile.open(mode="r:gz", fileobj=bytes_data)
    file_uploaded = True
    print('upload file done.')

## sidebar end.
st.title('selected connection')
st.session_state.selected_conn

if not st.session_state.selected_conn and not file_uploaded:
    st.error('需要上传离线数据包或者连接数据库...')
    st.stop()

is_cdb = True
version_12c = True
awr_history_days = 1
top_n_results = 15
# print("let's go...")
profile_placeholder = st.empty()
with profile_placeholder.container():
    df_inst = query_inst(st.session_state.selected_conn) if not file_uploaded else df_from_tar('instance')
    if df_inst is not None:
        if not file_uploaded:
            df2tar(df_inst, tar_file, 'instance.csv')

        df_inst = df_inst.rename(columns=str.lower)
        main_release = int(df_inst.head(n=1).loc[0,'version'].split('.')[0])
        if main_release < 12:
            version_12c = False
    # v$database
    db_df: pd.DataFrame = query_database(
        st.session_state.selected_conn) if not file_uploaded else df_from_tar('database')
    if not file_uploaded:
        df2tar(db_df, tar_file, 'database.csv')
    # st.write(df.info())
    if db_df is not None:
        db_df = db_df.rename(columns=str.upper)
        if version_12c:
            db_v_columns = ['DBID', 'NAME', 'LOG_MODE', 'CONTROLFILE_TYPE', 'OPEN_MODE', 'PROTECTION_MODE',
                        'PROTECTION_LEVEL', 'DATABASE_ROLE', 'SWITCHOVER_STATUS', 'SUPPLEMENTAL_LOG_DATA_MIN',
                        'SUPPLEMENTAL_LOG_DATA_PK', 'SUPPLEMENTAL_LOG_DATA_UI', 'FORCE_LOGGING', 'PLATFORM_NAME',
                        'FLASHBACK_ON', 'SUPPLEMENTAL_LOG_DATA_FK', 'SUPPLEMENTAL_LOG_DATA_ALL', 'DB_UNIQUE_NAME',
                        'CDB'] 
        else:
            db_v_columns = ['DBID', 'NAME', 'LOG_MODE', 'CONTROLFILE_TYPE', 'OPEN_MODE', 'PROTECTION_MODE',
                            'PROTECTION_LEVEL', 'DATABASE_ROLE', 'SWITCHOVER_STATUS', 'SUPPLEMENTAL_LOG_DATA_MIN',
                            'SUPPLEMENTAL_LOG_DATA_PK', 'SUPPLEMENTAL_LOG_DATA_UI', 'FORCE_LOGGING', 'PLATFORM_NAME',
                            'FLASHBACK_ON', 'SUPPLEMENTAL_LOG_DATA_FK', 'SUPPLEMENTAL_LOG_DATA_ALL', 'DB_UNIQUE_NAME']
        db_df = db_df[db_v_columns]
        
        is_archivelog = (db_df['LOG_MODE'][0] == 'ARCHIVELOG')
        is_cdb = version_12c and (db_df['CDB'][0] == 'YES')
        if is_cdb:
            df_pdb: pd.DataFrame = query_pdb(
                st.session_state.selected_conn) if not file_uploaded else df_from_tar('pdb-info')
            if not file_uploaded:
                df2tar(df_pdb, tar_file, 'pdb-info.csv')

            df_pdb=df_pdb.append(pd.DataFrame([[0,'Entire CDB']],columns=['con_id','name']))
            with pdb_choose_container.container():
                st.selectbox(label='选择PDB', options=pd.Series(
                    ['All']).append(df_pdb['name']), key='selected_pdb')

        db_df.rename(columns={'NAME': '数据库名',  'LOG_MODE': '归档模式', 'CONTROLFILE_TYPE': '控制文件类型', 'OPEN_MODE': '打开模式', 'PROTECTION_MODE': '保护模式', 'PROTECTION_LEVEL': '保护级别',
                            'DATABASE_ROLE': '数据库角色', 'SUPPLEMENTAL_LOG_DATA_MIN': '补充日志最小', 
                            'SUPPLEMENTAL_LOG_DATA_PK': '补充日志主键', 'SUPPLEMENTAL_LOG_DATA_UI': '补充日志UI', 
                            'FORCE_LOGGING': '强制日志', 'PLATFORM_NAME': '平台名称', 'FLASHBACK_ON': '闪回', 
                            'SUPPLEMENTAL_LOG_DATA_FK': '补充日志FK', 'SUPPLEMENTAL_LOG_DATA_ALL': '补充日志ALL'}, inplace=True)
        # db_df = db_df.melt(id_vars=('DBID', '数据库名'), var_name='配置', value_name='值')
        for col in db_df.columns:
            db_df[col] = db_df[col].astype(str)
        df2 = db_df.melt(id_vars=('DBID', '数据库名'), var_name='配置', value_name='值')
        # st.write(df2.info())
        st.header('数据库配置信息: '+db_df['数据库名'][0])
        st.write(df2[['配置','值']])

    # v$instance
    st.header('数据库实例信息')
    # display table of instance
    # df_inst =df_inst[['instance_name','host_name','version','startup_time','status','parallel']].rename(columns=translate_word) 
    # df_inst.columns
    st.write(df_inst[['instance_name','host_name','version','startup_time','status','parallel']].rename(columns=translate_word))

    # 补丁信息
    st.header('补丁信息')
    df_patch = query_patchinfo(
        st.session_state.selected_conn) if not file_uploaded else df_from_tar('patch-info')
    if not file_uploaded:
        df2tar(df_patch, tar_file, 'patch-info.csv')

    df_patch = df_patch.rename(columns=str.lower)
    st.write(df_patch.rename(columns=translate_word))

    df_mem = query_quest_memory(
        st.session_state.selected_conn) if not file_uploaded else df_from_tar('memory-info')
    if not file_uploaded:
        df2tar(df_mem, tar_file, 'memory-info.csv')

    sunburst_data = []
    layer_df(data=df_mem, parent_column='parent_memory_component', id_column='memory_component',
            name_label='name', value_label='value', name_property='memory_component',
            value_property='memory_size', store_dict=sunburst_data)
    # sunburst_data

    st.header('Oracle 内存分配')
    option = {
        'visualMap': {
            'type': 'continuous',
            'min': df_mem['memory_size'].min(),
            'max': df_mem['memory_size'].max(),
            'inRange': {
                'color': ['#2F93C8', '#AEC48F', '#FFDB5C', '#F98862']
            }
        },
        'series': {
            'type': 'sunburst',
            'data': sunburst_data,
            'radius': [0, '90%'],
            'label': {
                'rotate': 'radial'
            }
        }
    }

    st_echarts(options=option)

    if st.checkbox(label="查看内存信息原始数据"):
        st.write(df_mem.rename(columns=translate_word))

    # alert 日志
    st.header('Alert 日志位置')
    df_alert = query_alert(
        st.session_state.selected_conn) if not file_uploaded else df_from_tar('alert-dest')
    if not file_uploaded:
        df2tar(df_alert, tar_file, 'alert-dest.csv')

    st.write(df_alert.rename(columns=translate_word))

    # 归档模式
    if is_archivelog:
        st.header('归档路径信息')
        df_arl = query_arl_dest(
            st.session_state.selected_conn) if not file_uploaded else df_from_tar('arl-dest-info')
        if not file_uploaded:
            df2tar(df_arl, tar_file, 'arl-dest-info.csv')

        st.write(df_arl.rename(columns=translate_word))
        st.header('备份信息')
        df_bakup = query_rman_bakinfo(
            st.session_state.selected_conn) if not file_uploaded else df_from_tar('backup-info')
        if not file_uploaded:
            df2tar(df_bakup, tar_file, 'backup-info.csv')
        # if df_bakup.shape[0]>0:
        df_bakup['end_time'] = pd.to_datetime(df_bakup['end_time'])
        df_bakup['start_time'] = pd.to_datetime(df_bakup['start_time'])
        df_bakup['elapsed'] = df_bakup['end_time']-df_bakup['start_time']
        df_bakup['elapsed'] = df_bakup['elapsed'].apply(lambda x: str(round(pd.Timedelta(x).total_seconds()
                                                             % 86400.0 / 3600.0))+' Hr')
        # df_bakup.info()
        
        st.dataframe(df_bakup.rename(columns=translate_word))
    
    st.header('FRA 信息汇总(MB)')
    df_fra = query_fra(
        st.session_state.selected_conn) if not file_uploaded else df_from_tar('fra-info')
    if not file_uploaded:
        df2tar(df_fra, tar_file, 'fra-info.csv')

    df_fra['space_limit'] = int(df_fra['space_limit']/1048576)
    df_fra['space_used'] = int(df_fra['space_used']/1048576)
    st.write(df_fra.rename(columns=translate_word))

    st.header('初始化参数(数值型)')
    df_param = query_parameter(
        st.session_state.selected_conn) if not file_uploaded else df_from_tar('parameter-num')
    if not file_uploaded:
        df2tar(df_param, tar_file, 'parameter-num.csv')

    st.write(df_param.rename(columns=translate_word))

    st.header('初始化参数(其他)')
    df_param2 = query_parameter2(
        st.session_state.selected_conn) if not file_uploaded else df_from_tar('parameter-nonnum')
    if not file_uploaded:
        df2tar(df_param, tar_file, 'parameter-nonnum.csv')

    st.write(df_param2.rename(columns=translate_word))

    # expiring user
    st.header('密码将要过期的用户')
    df_expu = query_expu(st.session_state.selected_conn,
                         is_cdb) if not file_uploaded else df_from_tar('expiring-user')
    if not file_uploaded:
        df2tar(df_expu, tar_file, 'expiring-user.csv')

    st.write(df_expu.rename(columns=translate_word))

    st.header('用户及角色分配')
    df_users = query_user_privs(st.session_state.selected_conn,
                                is_cdb) if not file_uploaded else df_from_tar('user-privs')
    if not file_uploaded:
        df2tar(df_users, tar_file, 'user-privs.csv')
    
    if is_cdb:
        df_users = df_users.merge(
            df_pdb[['con_id', 'name']], on='con_id').fillna('')
        _s = filter_pdb(df_users, 'name')

        df_users = df_users.loc[_s, ['name', 'username', 'default_tablespace',
                         'temporary_tablespace', 'granted_role', 'default_role']]
    st.write(df_users.rename(columns=translate_word))

    st.header('Cursor使用效率')
    df_cur_eff = query_cursor_efficiency(st.session_state.selected_conn) if not file_uploaded else df_from_tar('cursor-efficiency')
    if not file_uploaded:
        df2tar(df_cur_eff, tar_file, 'cursor-efficiency.csv')
    if df_cur_eff is not None:
        st.write(df_cur_eff)
    else:
        st.warning('数据未存储')


if show_storage_file:
    st.header('控制文件')
    df_ctrl: pd.DataFrame = query_controlfile(
        st.session_state.selected_conn) if not file_uploaded else df_from_tar('controlfile')
    if not file_uploaded:
        df2tar(df_ctrl,tar_file, 'controlfile.csv')
    if df_ctrl is not None:
        st.write(df_ctrl.rename(columns=translate_word))
    else:
        st.warning('数据未存储')

    st.header('重做日志文件')
    df_redo = query_logfile(
        st.session_state.selected_conn) if not file_uploaded else df_from_tar('redo-log')
    if not file_uploaded:
        df2tar(df_redo, tar_file, 'redo-log.csv')
    if df_redo is not None:
        st.write(df_redo.rename(columns=translate_word))
    else:
        st.warning('数据未存储')

    # Database level DBfiles info
    st.header('数据库文件概览')
    df_dbfile: pd.DataFrame = query_dbfile(
        st.session_state.selected_conn) if not file_uploaded else df_from_tar('datafiles')
    if not file_uploaded:
        df2tar(df_dbfile, tar_file, 'datafiles.csv')

    if df_dbfile is not None:
        if is_cdb:
            df_dbfile = df_dbfile.merge(df_pdb, on='con_id')
            _s = filter_pdb(df_dbfile, 'name_y')
            df_dbfile = df_dbfile.loc[_s, ['name_y','name_x','FILE#','bytes','status','open_mode','enabled','TS#']]
            tbspc_cnt = df_dbfile.groupby(['name_y', 'TS#']).size().count()
        else:
            tbspc_cnt = df_dbfile.groupby(['TS#']).size().count()
        dbf_cnt = df_dbfile.shape[0]
        dbf_siz = df_dbfile['bytes'].sum() / 1048576   # MB


        st.write(pd.DataFrame(data={'数据文件数': [dbf_cnt,], '总大小(MB)': [dbf_siz,], '表空间数': tbspc_cnt}))
        st.write(df_dbfile.rename(columns=translate_word))
    else :
        st.warning('数据未存储')

    # permanent tablespace layout
    st.header('表空间使用(MB)')
    df_ptbspc = query_pts(st.session_state.selected_conn,
                          is_cdb) if not file_uploaded else df_from_tar('tablespace-usage')
    if not file_uploaded:
        df2tar(df_ptbspc, tar_file, 'tablespace-usage.csv')
    if df_ptbspc is not None:
        if is_cdb:
            df_ptbspc = df_ptbspc.merge(df_pdb[['con_id','name']], on='con_id')
            _s = filter_pdb(df_ptbspc, 'name')
            
            df_ptbspc = df_ptbspc.loc[_s, ['name','tablespace_name','file_size','file_max_size','file_free_space']]
        else:
            df_ptbspc = df_ptbspc[ 'tablespace_name',
                                    'file_size', 'file_max_size', 'file_free_space']
        df_ptbspc['file_size'] = df_ptbspc['file_size'].apply(lambda x: round(x/1048576,2))
        df_ptbspc['file_max_size'] = df_ptbspc['file_max_size'].apply(
            lambda x: round(x/1048576, 2))
        df_ptbspc['file_free_space'] = df_ptbspc['file_free_space'].apply(
            lambda x: round(x/1048576, 2))
        st.write(df_ptbspc.rename(columns=translate_word) )
    else:
        st.warning('数据未存储')

    # undo tablespace usage
    st.header('UNDO表空间使用(MB)')
    df_utbspc = query_uts(st.session_state.selected_conn,
                          is_cdb) if not file_uploaded else df_from_tar('undo-tablespace-usage')
    if not file_uploaded:
        df2tar(df_utbspc, tar_file, 'undo-tablespace-usage.csv')
    if df_utbspc is not None:
        if is_cdb:
            df_utbspc = df_utbspc.merge(df_pdb[['con_id', 'name']], on='con_id')
            _s = filter_pdb(df_utbspc, 'name')

            df_utbspc = df_utbspc.loc[_s, [
                'name', 'tablespace_name', 'file_size', 'file_max_size', 'file_free_space']]
        else:
            df_utbspc = df_utbspc['tablespace_name',
                                    'file_size', 'file_max_size', 'file_free_space']
        df_utbspc['file_size'] = df_utbspc['file_size'].apply(
            lambda x: round(x/1048576, 2))
        df_utbspc['file_max_size'] = df_utbspc['file_max_size'].apply(
            lambda x: round(x/1048576, 2))
        df_utbspc['file_free_space'] = df_utbspc['file_free_space'].apply(
            lambda x: round(x/1048576, 2))
        st.write(df_utbspc.rename(columns=translate_word))
    else:
        st.warning('数据未存储')

    # TEMP tablespace usage
    st.header('临时表空间使用(MB)')
    df_ttbspc = query_tts(st.session_state.selected_conn,
                          is_cdb) if not file_uploaded else df_from_tar('temp-tablespace-usage')
    if not file_uploaded:
        df2tar(df_ttbspc, tar_file, 'temp-tablespace-usage.csv')
    if df_ttbspc is not None:
        if is_cdb:
            df_ttbspc = df_ttbspc.merge(df_pdb[['con_id', 'name']], on='con_id')
            _s = filter_pdb(df_ttbspc, 'name')

            df_ttbspc = df_ttbspc.loc[_s, [
                'name', 'tablespace_name', 'param', 'value',]]
            df_ttbspc = pd.pivot_table(df_ttbspc, values='value', index=[
                                    'name', 'tablespace_name'], columns='param')
        else:
            df_ttbspc = df_ttbspc['tablespace_name',
                                    'param', 'value',]
            df_ttbspc = pd.pivot_table(df_ttbspc, values='value', index=[
                                        'tablespace_name'], columns='param')
        df_ttbspc['filesize'] = df_ttbspc['filesize'].apply(
            lambda x: round(x/1048576, 2))
        df_ttbspc['maxsize'] = df_ttbspc['maxsize'].apply(
            lambda x: round(x/1048576, 2))
        df_ttbspc['usedbytes'] = df_ttbspc['usedbytes'].apply(
            lambda x: round(x/1048576, 2))
        st.write(df_ttbspc.rename(columns=translate_word))
    else:
        st.warning('数据未存储')
    
    if st.checkbox('显示冗余的索引(需要能够访问sys用户基表权限，结果仅供参考，调整应在专家指导下进行)'):
        df_rdnt_idx = query_redundent_idx(st.session_state.selected_conn) if not file_uploaded else df_from_tar('redundent-indexes')
        if not file_uploaded:
            df2tar(df_rdnt_idx, tar_file, 'redundent-indexes.csv')
        if df_rdnt_idx is not None:
            st.write(df_rdnt_idx)
        else:
            st.warning('数据未存储')

if show_sql_info:
    sqlprof_placeholder = st.empty()
    with sqlprof_placeholder.container():
        st.header('SQL概要...')
        # SQL 概览 total_sql, sharable_mem, singleUseSql, singleUseSqlMem, totalCurs, cpuTime, elapseTime
        df_sqlarea = query_sqlarea(st.session_state.selected_conn,
                                   is_cdb) if not file_uploaded else df_from_tar('sqlarea-summary')
        if not file_uploaded:
            df2tar(df_sqlarea, tar_file, 'sqlarea-summary.csv')
        if df_sqlarea is not None:
            if is_cdb:
                df_sqlarea = df_sqlarea.merge(
                    df_pdb[['con_id', 'name']], on='con_id')
                _s = filter_pdb(df_sqlarea, 'name')

                df_sqlarea = df_sqlarea.loc[_s, ['name', 'instance_name', 'total_sql', 'totalsqlmem',
                                'singleusesql', 'singleusesqlmem', 'totalcurs', 'cputime', 'elapsetime']]
            else:
                df_sqlarea = df_sqlarea[['instance_name', 'total_sql', 'totalsqlmem',
                        'singleusesql', 'singleusesqlmem', 'totalcurs', 'cputime', 'elapsetime']]
            # df = df.merge(df_pdb,on="con_id")[['name','instance_name','total_sql','totalsqlmem','singleusesql','singleusesqlmem','totalcurs','cputime','elapsetime']]
            st.write(df_sqlarea.rename(columns=translate_word))
        else:
            st.warning('数据未存储')

    topsql_placeholder = st.empty()
    with topsql_placeholder.container():
        # Top SQL
        if st.checkbox('TOP SQL: 按照整体执行次数[executions]排序'):
            df_tsql_exec = query_topsql_exec(
                st.session_state.selected_conn, is_cdb) if not file_uploaded else df_from_tar('topsql-exec')
            if not file_uploaded:
                df2tar(df_tsql_exec, tar_file, 'topsql-exec.csv')
            render_topsql_section(df_tsql_exec, is_cdb, df_pdb)
            

        if st.checkbox('TOP SQL: 按照整体排序次数[sorts]排序'):
            df_tsql_sort = query_topsql_sorts(
                st.session_state.selected_conn, is_cdb) if not file_uploaded else df_from_tar('topsql-sort')
            if not file_uploaded:
                df2tar(df_tsql_sort, tar_file, 'topsql-sort.csv')
            render_topsql_section(df_tsql_sort, is_cdb, df_pdb)
            
        
        if st.checkbox('TOP SQL: 按照占用共享内存[sharable_mem]排序'):
            df_tsql_mem = query_topsql_shm(
                st.session_state.selected_conn, is_cdb) if not file_uploaded else df_from_tar('topsql-shmemory')
            if not file_uploaded:
                df2tar(df_tsql_mem, tar_file, 'topsql-shmemory.csv')
            render_topsql_section(df_tsql_mem, is_cdb, df_pdb)
            

        if st.checkbox('TOP SQL: 按照并发执行次数[px server executions]排序'):
            df_tsql_px = query_topsql_pxexec(
                st.session_state.selected_conn, is_cdb) if not file_uploaded else df_from_tar('topsql-pxexec')
            if not file_uploaded:
                df2tar(df_tsql_px, tar_file, 'topsql-pxexec.csv')
            render_topsql_section(df_tsql_px, is_cdb, df_pdb)
            

        if st.checkbox('TOP SQL: 按照失效次数[invalidations]排序'):
            df_tsql_invld = query_topsql_invalid(
                st.session_state.selected_conn, is_cdb) if not file_uploaded else df_from_tar('topsql-invalids')
            if not file_uploaded:
                df2tar(df_tsql_invld, tar_file, 'topsql-invalids.csv')
            render_topsql_section(df_tsql_invld, is_cdb, df_pdb)
            

        if st.checkbox('TOP SQL: 按照SQL解析次数[parse]排序'):
            df_tsql_prs = query_topsql_parse(
                st.session_state.selected_conn, is_cdb) if not file_uploaded else df_from_tar('topsql-parse')
            if not file_uploaded:
                df2tar(df_tsql_prs, tar_file, 'topsql-parse.csv')
            render_topsql_section(df_tsql_prs, is_cdb, df_pdb)
            
        
        if st.checkbox('TOP SQL: 按照磁盘读取次数[disk reads]排序'):
            df_tsql_phr = query_topsql_phr(
                st.session_state.selected_conn, is_cdb) if not file_uploaded else df_from_tar('topsql-phr-reads')
            if not file_uploaded:
                df2tar(df_tsql_phr, tar_file, 'topsql-phr-reads.csv')
            render_topsql_section(df_tsql_phr, is_cdb, df_pdb)
           
        
        if st.checkbox('TOP SQL: 按照内存读取次数[buffer gets]排序'):
            df_tsql_get = query_topsql_get(
                st.session_state.selected_conn, is_cdb) if not file_uploaded else df_from_tar('topsql-buffer-get')
            if not file_uploaded:
                df2tar(df_tsql_get, tar_file, 'topsql-buffer-get.csv')
            render_topsql_section(df_tsql_get, is_cdb, df_pdb)
 

        if st.checkbox('TOP SQL: 按照应用程序等待时间[application wait time]排序'):
            df_tsql_awt = query_topsql_awt(
                st.session_state.selected_conn, is_cdb) if not file_uploaded else df_from_tar('topsql-app-wait')
            if not file_uploaded:
                df2tar(df_tsql_awt, tar_file, 'topsql-app-wait.csv')
            render_topsql_section(df_tsql_awt, is_cdb, df_pdb)
            
        
        if st.checkbox('TOP SQL: 按照并发等待时间[concurrency wait time(锁资源)]排序'):
            df_tsql_conwt = query_topsql_conwt(
                st.session_state.selected_conn, is_cdb) if not file_uploaded else df_from_tar('topsql-concurrent-wait')
            if not file_uploaded:
                df2tar(df_tsql_conwt, tar_file, 'topsql-concurrent-wait.csv')
            render_topsql_section(df_tsql_conwt, is_cdb, df_pdb)

           

        if st.checkbox('TOP SQL: 按照集群等待时间[cluster wait time]排序'):
            df_tsql_cluwt = query_topsql_cluwt(
                st.session_state.selected_conn, is_cdb) if not file_uploaded else df_from_tar('topsql-cluster-wait')
            if not file_uploaded:
                df2tar(df_tsql_cluwt, tar_file, 'topsql-cluster-wait.csv')
            render_topsql_section(df_tsql_cluwt, is_cdb, df_pdb)

            

        if st.checkbox('TOP SQL: 按照用户磁盘IO等待时间[user io wait time]排序'):
            df_tsql_uiowt = query_topsql_uiowt(
                st.session_state.selected_conn, is_cdb) if not file_uploaded else df_from_tar('topsql-user-iowait')
            if not file_uploaded:
                df2tar(df_tsql_uiowt, tar_file, 'topsql-user-iowait.csv')
            render_topsql_section(df_tsql_uiowt, is_cdb, df_pdb)

           

        if st.checkbox('TOP SQL: 按照操作数据行数[rows processed]排序'):
            df_tsql_rows = query_topsql_rows(
                st.session_state.selected_conn, is_cdb) if not file_uploaded else df_from_tar('topsql-rows')
            if not file_uploaded:
                df2tar(df_tsql_rows, tar_file, 'topsql-rows.csv')
            render_topsql_section(df_tsql_rows, is_cdb, df_pdb)

            # if df_tsql_rows is not None:
            #     if is_cdb:
            #         df_tsql_rows = df_tsql_rows.merge(
            #             df_pdb[['con_id', 'name']], on='con_id')
            #         _s = filter_pdb(df_tsql_rows, 'name')
            #         df_tsql_rows = df_tsql_rows.loc[_s, ['name', 'sql_id', 'value', 'sql_text',
            #                         ]]
            #     else:
            #         df_tsql_rows = df_tsql_rows[['sql_id', 'value', 'sql_text']]

            #     st.write(df_tsql_rows.rename(columns=translate_word))
            # else:
            #     st.warning('数据未存储')
        
        if st.checkbox('TOP SQL: 按照消耗cpu时间[CPU time]排序'):
            df_tsql_cpu = query_topsql_cpu(
                st.session_state.selected_conn, is_cdb) if not file_uploaded else df_from_tar('topsql-cpu')
            if not file_uploaded:
                df2tar(df_tsql_cpu, tar_file, 'topsql-cpu.csv')
            render_topsql_section(df_tsql_cpu, is_cdb, df_pdb)

            # if df_tsql_cpu is not None:
            #     if is_cdb:
            #         df_tsql_cpu = df_tsql_cpu.merge(
            #             df_pdb[['con_id', 'name']], on='con_id')
            #         _s = filter_pdb(df_tsql_cpu, 'name')
            #         df_tsql_cpu = df_tsql_cpu.loc[_s, ['name', 'sql_id', 'value', 'sql_text',
            #                         ]]
            #     else:
            #         df_tsql_cpu = df_tsql_cpu[['sql_id', 'value', 'sql_text']]

            #     st.write(df_tsql_cpu.rename(columns=translate_word))
            # else:
            #     st.warning('数据未存储')
        
        if st.checkbox('TOP SQL: 按照执行时间[elapsed time]排序'):
            df_tsql_elps = query_topsql_elps(
                st.session_state.selected_conn, is_cdb) if not file_uploaded else df_from_tar('topsql-elapse')
            if not file_uploaded:
                df2tar(df_tsql_elps, tar_file, 'topsql-elapse.csv')
            render_topsql_section(df_tsql_elps, is_cdb, df_pdb)

            # if df_tsql_elps is not None:
            #     if is_cdb:
            #         df_tsql_elps = df_tsql_elps.merge(
            #             df_pdb[['con_id', 'name']], on='con_id')
            #         _s = filter_pdb(df_tsql_elps, 'name')
            #         df_tsql_elps = df_tsql_elps.loc[_s, ['name', 'sql_id', 'value', 'sql_text',
            #                         ]]
            #     else:
            #         df_tsql_elps = df_tsql_elps[['sql_id', 'value', 'sql_text']]

            #     st.write(df_tsql_elps.rename(columns=translate_word))
            # else:
            #     st.warning('数据未存储')

        ## per execution statistics
        if st.checkbox('TOP SQL: 按照平均排序次数[sorts/exec#]排序'):
            df_tsql_sort_pe = query_topsql_sortspe(
                st.session_state.selected_conn, is_cdb) if not file_uploaded else df_from_tar('topsql-sort-exec')
            if not file_uploaded:
                df2tar(df_tsql_sort_pe, tar_file, 'topsql-sort-exec.csv')
            render_topsql_section(df_tsql_sort_pe, is_cdb, df_pdb)

            # if df_tsql_sort_pe is not None:
            #     if is_cdb:
            #         df_tsql_sort_pe = df_tsql_sort_pe.merge(
            #             df_pdb[['con_id', 'name']], on='con_id')
            #         _s = filter_pdb(df_tsql_sort_pe, 'name')

            #         df_tsql_sort_pe = df_tsql_sort_pe.loc[_s, ['name', 'sql_id', 'value', 'sql_text',
            #                         ]]
            #     else:
            #         df_tsql_sort_pe = df_tsql_sort_pe[[
            #             'sql_id', 'value', 'sql_text']]

            #     st.write(df_tsql_sort_pe.rename(columns=translate_word))
            # else:
            #     st.warning('数据未存储')

        
        if st.checkbox('TOP SQL: 按照平均解析次数[parse/exec#]排序'):
            df_tsql_prs_pe = query_topsql_parsepe(
                st.session_state.selected_conn, is_cdb) if not file_uploaded else df_from_tar('topsql-parse-exec')
            if not file_uploaded:
                df2tar(df_tsql_prs_pe, tar_file, 'topsql-parse-exec.csv')
            render_topsql_section(df_tsql_sort_pe, is_cdb, df_pdb)

            # if df_tsql_prs_pe is not None:
            #     if is_cdb:
            #         df_tsql_prs_pe = df_tsql_prs_pe.merge(
            #             df_pdb[['con_id', 'name']], on='con_id')
            #         _s = filter_pdb(df_tsql_prs_pe, 'name')

            #         df_tsql_prs_pe = df_tsql_prs_pe.loc[_s, ['name', 'sql_id', 'value', 'sql_text',
            #                         ]]
            #     else:
            #         df_tsql_prs_pe = df_tsql_prs_pe[[
            #             'sql_id', 'value', 'sql_text']]

            #     st.write(df_tsql_prs_pe.rename(columns=translate_word))
            # else:
            #     st.warning('数据未存储')

        if st.checkbox('TOP SQL: 按照平均磁盘读取次数[disk reads/exec#]排序'):
            df_tsql_phr_pe = query_topsql_phrpe(
                st.session_state.selected_conn, is_cdb) if not file_uploaded else df_from_tar('topsql-phread-exec')
            if not file_uploaded:
                df2tar(df_tsql_phr_pe, tar_file, 'topsql-phread-exec.csv')
            render_topsql_section(df_tsql_phr_pe, is_cdb, df_pdb)

            # if df_tsql_phr_pe is not None:
            #     if is_cdb:
            #         df_tsql_phr_pe = df_tsql_phr_pe.merge(
            #             df_pdb[['con_id', 'name']], on='con_id')
            #         _s = filter_pdb(df_tsql_phr_pe, 'name')

            #         df_tsql_phr_pe = df_tsql_phr_pe.loc[_s, ['name', 'sql_id', 'value', 'sql_text',
            #                         ]]
            #     else:
            #         df_tsql_phr_pe = df_tsql_phr_pe[[
            #             'sql_id', 'value', 'sql_text']]

            #     st.write(df_tsql_phr_pe.rename(columns=translate_word))
            # else:
            #     st.warning('数据未存储')

        if st.checkbox('TOP SQL: 按照平均内存读取次数[buffer gets/exec#]排序'):
            df_tsql_get_pe = query_topsql_getpe(
                st.session_state.selected_conn, is_cdb) if not file_uploaded else df_from_tar('topsql-gets-exec')
            if not file_uploaded:
                df2tar(df_tsql_get_pe, tar_file, 'topsql-gets-exec.csv')
            render_topsql_section(df_tsql_get_pe, is_cdb, df_pdb)

            # if df_tsql_get_pe is not None:
            #     if is_cdb:
            #         df_tsql_get_pe = df_tsql_get_pe.merge(
            #             df_pdb[['con_id', 'name']], on='con_id')
            #         _s = filter_pdb(df_tsql_get_pe, 'name')

            #         df_tsql_get_pe = df_tsql_get_pe.loc[_s, ['name', 'sql_id', 'value', 'sql_text',
            #                         ]]
            #     else:
            #         df_tsql_get_pe = df_tsql_get_pe[[
            #             'sql_id', 'value', 'sql_text']]

            #     st.write(df_tsql_get_pe.rename(columns=translate_word))
            # else:
            #     st.warning('数据未存储')

        if st.checkbox('TOP SQL: 按照平均应用程序等待时间[application wait time/exec#]排序'):
            df_tsql_awt_pe = query_topsql_awtpe(
                st.session_state.selected_conn, is_cdb) if not file_uploaded else df_from_tar('topsql-appwait-exec')
            if not file_uploaded:
                df2tar(df_tsql_awt_pe, tar_file, 'topsql-appwait-exec.csv')
            render_topsql_section(df_tsql_awt_pe, is_cdb, df_pdb)

            # if df_tsql_awt_pe is not None:
            #     if is_cdb:
            #         df_tsql_awt_pe = df_tsql_awt_pe.merge(
            #             df_pdb[['con_id', 'name']], on='con_id')
            #         _s = filter_pdb(df_tsql_awt_pe, 'name')
            #         df_tsql_awt_pe = df_tsql_awt_pe.loc[_s, ['name', 'sql_id', 'value', 'sql_text',
            #                         ]]
            #     else:
            #         df_tsql_awt_pe = df_tsql_awt_pe[[
            #             'sql_id', 'value', 'sql_text']]

            #     st.write(df_tsql_awt_pe.rename(columns=translate_word))
            # else:
            #     st.warning('数据未存储')

        if st.checkbox('TOP SQL: 按照平均并发等待时间[concurrency wait time/exec#(锁资源)]排序'):
            df_tsql_conwt_pe = query_topsql_conwt_pe(
                st.session_state.selected_conn, is_cdb) if not file_uploaded else df_from_tar('topsql-concurrwait-exec')
            if not file_uploaded:
                df2tar(df_tsql_conwt_pe, tar_file, 'topsql-concurrwait-exec.csv')
            render_topsql_section(df_tsql_conwt_pe, is_cdb, df_pdb)

            # if df_tsql_conwt_pe is not None:
            #     if is_cdb:
            #         df_tsql_conwt_pe = df_tsql_conwt_pe.merge(
            #             df_pdb[['con_id', 'name']], on='con_id')
            #         _s = filter_pdb(df_tsql_conwt_pe, 'name')
            #         df_tsql_conwt_pe = df_tsql_conwt_pe.loc[_s, ['name', 'sql_id', 'value', 'sql_text',
            #                         ]]
            #     else:
            #         df_tsql_conwt_pe = df_tsql_conwt_pe[[
            #             'sql_id', 'value', 'sql_text']]

            #     st.write(df_tsql_conwt_pe.rename(columns=translate_word))
            # else:
            #     st.warning('数据未存储')

        if st.checkbox('TOP SQL: 按照平均集群等待时间[cluster wait time/exec#]排序'):
            df_tsql_cluwt_pe = query_topsql_cluwt_pe(
                st.session_state.selected_conn, is_cdb) if not file_uploaded else df_from_tar('topsql-clustwait-exec')
            if not file_uploaded:
                df2tar(df_tsql_cluwt_pe, tar_file, 'topsql-clustwait-exec.csv')
            render_topsql_section(df_tsql_cluwt_pe, is_cdb, df_pdb)

            # if df_tsql_cluwt_pe is not None:
            #     if is_cdb:
            #         df_tsql_cluwt_pe = df_tsql_cluwt_pe.merge(
            #             df_pdb[['con_id', 'name']], on='con_id')
            #         _s = filter_pdb(df_tsql_cluwt_pe, 'name')
            #         df_tsql_cluwt_pe = df_tsql_cluwt_pe.loc[_s, ['name', 'sql_id', 'value', 'sql_text',
            #                         ]]
            #     else:
            #         df_tsql_cluwt_pe = df_tsql_cluwt_pe[[
            #             'sql_id', 'value', 'sql_text']]

            #     st.write(df_tsql_cluwt_pe.rename(columns=translate_word))
            # else:
            #     st.warning('数据未存储')

        if st.checkbox('TOP SQL: 按照平均用户磁盘IO等待时间[user io wait time/exec#]排序'):
            df_tsql_uiowt_pe = query_topsql_uiowt_pe(
                st.session_state.selected_conn, is_cdb) if not file_uploaded else df_from_tar('topsql-userio-exec')
            if not file_uploaded:
                df2tar(df_tsql_uiowt_pe, tar_file, 'topsql-userio-exec.csv')
            render_topsql_section(df_tsql_uiowt_pe, is_cdb, df_pdb)

            # if df_tsql_uiowt_pe is not None:
            #     if is_cdb:
            #         df_tsql_uiowt_pe = df_tsql_uiowt_pe.merge(
            #             df_pdb[['con_id', 'name']], on='con_id')
            #         _s = filter_pdb(df_tsql_uiowt_pe, 'name')
            #         df_tsql_uiowt_pe = df_tsql_uiowt_pe.loc[_s, ['name', 'sql_id', 'value', 'sql_text',
            #                         ]]
            #     else:
            #         df_tsql_uiowt_pe = df_tsql_uiowt_pe[[
            #             'sql_id', 'value', 'sql_text']]

            #     st.write(df_tsql_uiowt_pe.rename(columns=translate_word))
            # else:
            #     st.warning('数据未存储')

        if st.checkbox('TOP SQL: 按照平均操作数据行数[rows processed/exec#]排序'):
            df_tsql_row_pe = query_topsql_rows_pe(
                st.session_state.selected_conn, is_cdb) if not file_uploaded else df_from_tar('topsql-rows-exec')
            if not file_uploaded:
                df2tar(df_tsql_row_pe, tar_file, 'topsql-rows-exec.csv')
            render_topsql_section(df_tsql_uiowt_pe, is_cdb, df_pdb)

            # if df_tsql_row_pe is not None:
            #     if is_cdb:
            #         df_tsql_row_pe = df_tsql_row_pe.merge(
            #             df_pdb[['con_id', 'name']], on='con_id')
            #         _s = filter_pdb(df_tsql_row_pe, 'name')
            #         df_tsql_row_pe = df_tsql_row_pe.loc[_s, ['name', 'sql_id', 'value', 'sql_text',
            #                         ]]
            #     else:
            #         df_tsql_row_pe = df_tsql_row_pe[[
            #             'sql_id', 'value', 'sql_text']]

            #     st.write(df_tsql_row_pe.rename(columns=translate_word))
            # else:
            #     st.warning('数据未存储')

        if st.checkbox('TOP SQL: 按照平均消耗cpu时间[CPU time/exec#]排序'):
            df_tsql_cpu_pe = query_topsql_cpu_pe(
                st.session_state.selected_conn, is_cdb) if not file_uploaded else df_from_tar('topsql-cpu-exec')
            if not file_uploaded:
                df2tar(df_tsql_cpu_pe, tar_file, 'topsql-cpu-exec.csv')
            render_topsql_section(df_tsql_cpu_pe, is_cdb, df_pdb)

            # if df_tsql_cpu_pe is not None:
            #     if is_cdb:
            #         df_tsql_cpu_pe = df_tsql_cpu_pe.merge(
            #             df_pdb[['con_id', 'name']], on='con_id')
            #         _s = filter_pdb(df_tsql_cpu_pe, 'name')
            #         df_tsql_cpu_pe = df_tsql_cpu_pe.loc[_s, ['name', 'sql_id', 'value', 'sql_text',
            #                         ]]
            #     else:
            #         df_tsql_cpu_pe = df_tsql_cpu_pe[[
            #             'sql_id', 'value', 'sql_text']]

            #     st.write(df_tsql_cpu_pe.rename(columns=translate_word))
            # else:
            #     st.warning('数据未存储')

        if st.checkbox('TOP SQL: 按照平均执行时间[elapsed time/exec#]排序'):
            df_tsql_elps_pe = query_topsql_elps_pe(
                st.session_state.selected_conn, is_cdb) if not file_uploaded else df_from_tar('topsql-elapsed-exec')
            if not file_uploaded:
                df2tar(df_tsql_elps_pe, tar_file, 'topsql-elapsed-exec.csv')
            render_topsql_section(df_tsql_elps_pe, is_cdb, df_pdb)

            # if df_tsql_elps_pe is not None:
            #     if is_cdb:
            #         df_tsql_elps_pe = df_tsql_elps_pe.merge(
            #             df_pdb[['con_id', 'name']], on='con_id')
            #         _s = filter_pdb(df_tsql_elps_pe, 'name')
            #         df_tsql_elps_pe = df_tsql_elps_pe.loc[_s, ['name', 'sql_id', 'value', 'sql_text',
            #                         ]]
            #     else:
            #         df_tsql_elps_pe = df_tsql_elps_pe[[
            #             'sql_id', 'value', 'sql_text']]

            #     st.write(df_tsql_elps_pe.rename(columns=translate_word))
            # else:
            #     st.warning('数据未存储')
        ## End

if show_io_statistics:
    fileio_placeholder = st.empty()
    with fileio_placeholder.container():
        st.header('数据库文件IO统计')
        df_fio = query_fileio(st.session_state.selected_conn,
                              is_cdb) if not file_uploaded else df_from_tar('dbfile-iostat')
        if not file_uploaded:
            df2tar(df_fio, tar_file, 'dbfile-iostat.csv')
        if df_fio is not None:
            if is_cdb:
                df_fio = df_fio.merge(df_pdb[['con_id', 'name']], on='con_id')
                _s = filter_pdb(df_fio, 'name')
                df_fio = df_fio.loc[_s, ['name', 'file_name', 'phyrds', 'singleblkrds', 'readtim', 'writetim',
                            'singleblkrdtim','avgiotm','lstiotim','miniotim','maxiortm','maxiowtm']]
            else:
                df_fio = df_fio[['file_name', 'phyrds', 'singleblkrds', 'readtim', 'writetim',
                        'singleblkrdtim', 'avgiotm', 'lstiotim', 'miniotim', 'maxiortm', 'maxiowtm']]
            st.write(df_fio.rename(columns=translate_word))
        else:
            st.warning('数据未存储')

        st.header('数据库临时文件IO统计')
        df_tfio = query_tmpio(st.session_state.selected_conn,
                              is_cdb) if not file_uploaded else df_from_tar('tempfile-iostat')
        if not file_uploaded:
            df2tar(df_tfio, tar_file, 'tempfile-iostat.csv')
        if df_tfio is not None:
            if is_cdb:
                df_tfio = df_tfio.merge(df_pdb[['con_id', 'name']], on='con_id')
                _s = filter_pdb(df_tfio, 'name')
                df_tfio = df_tfio.loc[_s, ['name', 'file_name', 'phyrds', 'singleblkrds', 'readtim', 'writetim',
                            'singleblkrdtim', 'avgiotm', 'lstiotim', 'miniotim', 'maxiortm', 'maxiowtm']]
            else:
                df_tfio = df_tfio[['file_name', 'phyrds', 'singleblkrds', 'readtim', 'writetim',
                        'singleblkrdtim', 'avgiotm', 'lstiotim', 'miniotim', 'maxiortm', 'maxiowtm']]
            st.write(df_tfio.rename(columns=translate_word))
        else:
            st.warning('数据未存储')


if show_current_sessions:
    st.header('查看当前会话信息')
    df_c_sess: pd.DataFrame = query_curr_sessions(
        st.session_state.selected_conn) if not file_uploaded else df_from_tar('current-sessions')
    if not file_uploaded:
        df2tar(df_c_sess, tar_file, 'current-sessions.csv')
    if df_c_sess is not None:
        sess_status = st.selectbox(label='选择会话状态', options=['ALL','ACTIVE','INACTIVE'])
        if is_cdb:
            df_c_sess = df_c_sess.merge(df_pdb[['con_id', 'name']], on='con_id')
            _s = filter_pdb(df_c_sess, 'name')
            df_c_sess = df_c_sess.loc[_s, ['name', 'inst_id', 'sid', 'SERIAL#', 'username', 'status',
                'server','schemaname','osuser','process','machine','port','program','sql_id',
                'sql_child_number','sql_exec_start','prev_sql_id','prev_child_number',
                'prev_exec_start','ROW_WAIT_OBJ#','ROW_WAIT_FILE#','logon_time',
                'blocking_session_status','blocking_instance','blocking_session',
                'final_blocking_session_status','final_blocking_instance',
                'final_blocking_session','event','p1text','p1','p2text','p2','wait_class']]
        else:
            columns = ['name', 'inst_id', 'sid', 'SERIAL#', 'username',
                    'status', 'event', 'p1text', 'p1', 'p2text', 'p2', 'wait_class', 'sql_id', 'ROW_WAIT_OBJ#', 'ROW_WAIT_FILE#', 'logon_time', 'blocking_session_status', 'blocking_instance', 'blocking_session',
                    'final_blocking_session_status', 'final_blocking_instance',
                    'final_blocking_session'
                    ] if main_release >= 11 else ['name',  'sid', 'SERIAL#', 'username',
                                                    'status', 'event', 'p1text', 'p1', 'p2text', 'p2', 'wait_class', 'sql_id', 'ROW_WAIT_OBJ#', 'ROW_WAIT_FILE#', 'logon_time', 'blocking_session_status',  'blocking_session', ]
            df_c_sess = df_c_sess[columns]
        
        df_c_sess['inst_id'] = df_c_sess['inst_id'].astype('int32')
        df_c_sess['sid'] = df_c_sess['sid'].astype('int32')
        df_c_sess['blocking_instance'] = df_c_sess['blocking_instance'].fillna(0).astype('int32')
        df_c_sess['blocking_session'] = df_c_sess['blocking_session'].fillna(0).astype(
            'int32')
        df_c_sess['SERIAL#'] = df_c_sess['SERIAL#'].astype('int32')

        if sess_status and sess_status != 'ALL':
            df_c_sess = df_c_sess.loc[df_c_sess['status'] == sess_status]
        st.write(df_c_sess.rename(columns=translate_word))
        # fig, ax = plt.subplots()
        container = st.empty()
        with container.container():
            col1,col2,col3 = st.columns(3)
            with col1:
                grp = df_c_sess.groupby(
                    ['username', 'machine']).size().reset_index()
                
                grp['cnt']=grp[0]
                del grp[0]
                
                grp = grp.sort_values(by=['cnt'], ascending=False).head(n=10)
                options = {
                    "title": {"text": "用户-machine", "subtext": "会话分布", "left": "center"},
                    "tooltip": {"trigger": "item"},
                    "legend": {"orient": "vertical", "left": "left", },
                    "series": [
                        {
                            "name": "访问来源",
                            "type": "pie",
                            "radius": "50%",
                            "data": [
                                {"value": item['cnt'], "name": item['username']+'-'+item['machine']}
                                    for _, item in grp.iterrows()

                                # {"value": 1048, "name": "搜索引擎"},
                                # {"value": 735, "name": "直接访问"},
                                # {"value": 580, "name": "邮件营销"},
                                # {"value": 484, "name": "联盟广告"},
                                # {"value": 300, "name": "视频广告"},
                            ],
                            "emphasis": {
                                "itemStyle": {
                                    "shadowBlur": 10,
                                    "shadowOffsetX": 0,
                                    "shadowColor": "rgba(0, 0, 0, 0.5)",
                                }
                            },
                        }
                    ],
                }
                st_echarts(
                    options=options #, height="600px",
                )
                
                # plt.pie(grp.size(), labels=grp.size().index, autopct='%.2f%%')
                # plt.title("Sessions: username+machine dist...")
                # st.pyplot(plt)
            
            with col2:
                grp = df_c_sess.groupby(['event'])
                grp = grp.size().reset_index()
                grp['cnt'] = grp[0]
                del grp[0]
                grp = grp.sort_values(by=['cnt'], ascending=False).head(n=10)
                options = {
                    "title": {"text": "等待事件", "subtext": "会话分布", "left": "center"},
                    "tooltip": {"trigger": "item"},
                    "legend": {"orient": "vertical", "left": "left", },
                    "series": [
                        {
                            "name": "访问来源",
                            "type": "pie",
                            "radius": "50%",
                            "data": [
                                {"value": item['cnt'],
                                    "name": item['event']}
                                for _, item in grp.iterrows()

                                # {"value": 1048, "name": "搜索引擎"},
                                # {"value": 735, "name": "直接访问"},
                                # {"value": 580, "name": "邮件营销"},
                                # {"value": 484, "name": "联盟广告"},
                                # {"value": 300, "name": "视频广告"},
                            ],
                            "emphasis": {
                                "itemStyle": {
                                    "shadowBlur": 10,
                                    "shadowOffsetX": 0,
                                    "shadowColor": "rgba(0, 0, 0, 0.5)",
                                }
                            },
                        }
                    ],
                }
                st_echarts(
                    options=options  # , height="600px",
                )
                # plt.pie(grp.size(), labels=grp.size().index, autopct='%.2f%%')
                # plt.title("Sessions: wait event dist...")
                # st.pyplot(plt)
            
            with col3:
                grp = df_c_sess.groupby(['sql_id'])
                grp = grp.size().reset_index()
                grp['cnt'] = grp[0]
                del grp[0]
                grp = grp.sort_values(by=['cnt'], ascending=False).head(n=10)
                options = {
                    "title": {"text": "SQL ID", "subtext": "会话分布", "left": "center"},
                    "tooltip": {"trigger": "item"},
                    "legend": {"orient": "vertical", "left": "left", },
                    "series": [
                        {
                            "name": "session分布",
                            "type": "pie",
                            "radius": "50%",
                            "data": [
                                {"value": item['cnt'],
                                    "name": item['sql_id']}
                                for _, item in grp.iterrows()

                            ],
                            "emphasis": {
                                "itemStyle": {
                                    "shadowBlur": 10,
                                    "shadowOffsetX": 0,
                                    "shadowColor": "rgba(0, 0, 0, 0.5)",
                                }
                            },
                        }
                    ],
                }
                st_echarts(
                    options=options  # , height="600px",
                )

        # blocking session tree.
        if ('blocking_instance' in df_c_sess.columns.values.tolist()) \
                and ('blocking_session' in df_c_sess.columns.values.tolist()):
            if 'name' not in df_c_sess:
                df_c_sess['name'] = df_c_sess.assign('')
            st.subheader('会话之间的堵塞关系')
            df_c_sess['info'] = df_c_sess['name']+',实例:' + \
                df_c_sess['inst_id'].astype(str)+',sid/serial#:'+df_c_sess['sid'].astype(str)+','+df_c_sess['SERIAL#'].astype(str)+',\n用户名:'+df_c_sess['username']+'状态:'+df_c_sess['status'] +'\n等待:' +df_c_sess['event']+'SQL:'+df_c_sess['sql_id']
            df_c_sess['uid'] = df_c_sess['inst_id'].astype(str)+'_'+df_c_sess['sid'].astype(str)
            df_c_sess['pid'] = df_c_sess.apply(
                lambda x: str(x['blocking_instance'])+'_'+str(x['blocking_session']) if x['blocking_session'] else None, axis=1)
            # (df_c_sess['blocking_instance'].astype(str)+'_'+df_c_sess['blocking_session'].astype(str)
                                # ) 
            store_data = []
            layer_df(data=df_c_sess[['uid','pid','info','sid','SERIAL#']], parent_column='pid', id_column='uid', 
                name_label='name', value_label='value',name_property="info",value_property="sid",
                    store_dict=store_data, current_value= None)
            store_dict = {
                'name': '会话相互堵塞情况展示树:',
                'value': 0,
                'children': [item for item in store_data if item['children']]
            }
            # st.write(store_dict)
            option = {
                 'tooltip': {
                     'trigger': 'item',
                     'triggerOn': 'mousemove'
                     },
                'series': [
                    {
                        'type': 'tree',
                        'data': [store_dict],
                        'top': '1%',
                        'left': '7%',
                        'bottom': '1%',
                        'right': '20%',
                        'symbolSize': 7,
                        'label': {
                            'position': 'left',
                            'verticalAlign': 'middle',
                            'align': 'right',
                            'fontSize': 12
                        },
                        'leaves': {
                            'label': {
                                'position': 'right',
                                'verticalAlign': 'middle',
                                'align': 'left'
                            }
                        },
                        'emphasis': {
                            'focus': 'descendant'
                        },
                        'expandAndCollapse': True,
                        'animationDuration': 550,
                        'animationDurationUpdate': 750
                    }
                ]
            }
            st_echarts(options = option)
        
        else:
            st.warning('数据库版本太旧了吧...')
    else:
        st.warning('数据未存储')
    
    if st.checkbox('显示当前未提交事务信息(如果发现有会话状态为INACTIVE,应引起注意)'):
        df_curr_tx: pd.DataFrame = query_uncommit_tx(
            st.session_state.selected_conn, is_cdb) if not file_uploaded else df_from_tar('uncommit-tx')
        if not file_uploaded:
            df2tar(df_curr_tx, tar_file, 'uncommit-tx.csv')
        
        if df_curr_tx is not None:
            if is_cdb:
                df_curr_tx = df_curr_tx.merge(
                    df_pdb[['con_id', 'name']], on='con_id')
                _s = filter_pdb(df_curr_tx, 'name')
                df_curr_tx = df_curr_tx.loc[_s, ['name', 'inst_id', 'sid', 'start_time',
                                                 'username',  'status', 'sess_status',  'ubablk',]]
            else:
                df_curr_tx = df_curr_tx[['inst_id', 'sid', 'start_time', 'username',
                                         'status', 'sess_status', 'ubablk']]
            st.write(df_curr_tx.rename(columns={'name': 'PDB','inst_id':'实例','sid':'sid','start_time':'事务开始','username':'用户名','status':'事务状态','sess_status':'会话状态'}))
        else:
            st.warning('数据未存储')

if show_awr_loads:
    st.header('历史负载情况')
    df_load: pd.DataFrame = query_perf_load(
        st.session_state.selected_conn, datetime.datetime.now() - datetime.timedelta(7), datetime.datetime.now(), is_cdb) if not file_uploaded else df_from_tar('database-perf-load')
    if not file_uploaded:
        df2tar(df_load, tar_file, 'database-perf-load.csv')
    if df_load is not None:
        if is_cdb:
            df_load = df_load.merge(df_pdb[['con_id', 'name']], on='con_id')
            _s = filter_pdb(df_load, 'name')
            df_load = df_load.loc[_s, ['name', 'ten_min_timestr', 'sess_count']]
        else:
            df_load = df_load[['ten_min_timestr', 'sess_count']]

        df_load['sess_count'] = df_load['sess_count'].astype(float)

        df_load = df_load.pivot(index='ten_min_timestr',
                                columns='name', values='sess_count').fillna(0)

        if st.checkbox(label='查看原始数据',key="awr-sess-load"):
            st.write(df_load.rename(columns=translate_word))
        

        c = (Line().add_xaxis(df_load.index.values.tolist()).set_global_opts(
            title_opts=opts.TitleOpts(title="Oracle-历史负载")))
        for col in df_load:
            c.add_yaxis(col, df_load[col].values.tolist(), is_smooth=True,
                        stack='Load', label_opts=opts.LabelOpts(is_show=False))
        st_pyecharts(c, theme=ThemeType.CHALK)
    else:
        st.warning('数据未存储')
    
    st.header('历史负载按照等待类别(wait class)分类')
    df_ash_tmodel = query_ash_timemodel(st.session_state.selected_conn, datetime.datetime.now(
    ) - datetime.timedelta(7), datetime.datetime.now(), is_cdb) if not file_uploaded else df_from_tar('ash-time-model')
    if not file_uploaded:
        df2tar(df_ash_tmodel, tar_file, 'ash-time-model.csv')
    if df_ash_tmodel is not None:
        if is_cdb:
            df_ash_tmodel = df_ash_tmodel.merge(
                df_pdb[['con_id', 'name']], on='con_id')
            _s = filter_pdb(df_ash_tmodel, 'name')
            df_ash_tmodel = df_ash_tmodel.loc[_s, [
                'name', 'tm', 'state', 'sess_cnt']]
        else:
            df_ash_tmodel = df_ash_tmodel[['tm', 'state', 'sess_cnt']]

        if st.checkbox(label='查看原始数据', key='awr-load-wait-class'):    
            st.write(df_ash_tmodel.rename(columns=translate_word))
        df_ash_tmodel['sess_cnt'] = df_ash_tmodel['sess_cnt'].astype(
            float).fillna(0).round(0)
        df_ash_tmodel = df_ash_tmodel.pivot_table(
            index=['tm', 'state'], columns='name', values='sess_cnt').reset_index().fillna(0)
        
        
        ## chart begin
        colors = {'IO': 'red', 'WAIT': 'green', 'CPU': 'black', 'CLUSTER': 'blue'}
        data = {}
        for state_value in df_ash_tmodel['state'].unique():
            data[state_value] = df_ash_tmodel[df_ash_tmodel['state'] == state_value]
        option = {
            'tooltip': {
                'trigger': 'axis',
                # 'label': '{b}',
                'axisPointer': {
                    'type': 'shadow'
                }
            },
            'legend': {
                'bottom':'bottom'
            },
            'grid': {
                'left': '3%',
                'right': '4%',
                'bottom': '20%',
                'containLabel': True
            },
            'xAxis': [
                {
                    'type': 'category',
                    'data': df_ash_tmodel['tm'].unique().tolist()
                }
            ],
            'yAxis': [
                {
                    'type': 'value'
                }
            ],
            'series': [
                
                {
                    'name': c+':'+state ,
                    'type': 'bar',
                    # 'color': colors[state],
                    'stack': c,
                    'label': {
                        # 'show': True,
                        # 'formatter': state
                    },
                    'tooltip': {
                        'show': True,
                        'formatter': state,
                        'valueFormatter': state,
                    },
                    'emphasis': {
                        'focus': 'series'
                    },
                    'data': df_state[c].values.tolist()
                } for state, df_state in data.items()
                    for c in df_state if c not in ('tm','state')
            ]
        }
        # option
        st_echarts(options=option)
        ## chart end
    else:
        st.warning('数据未存储')

    if st.checkbox('显示AWR中Latch信息'):
        st.subheader('AWR Latch等待',anchor="awr-latch-wait")
        df_awr_latch = query_awr_latch(st.session_state.selected_conn, datetime.datetime.now(
        ) - datetime.timedelta(1), datetime.datetime.now(), is_cdb) if not file_uploaded else df_from_tar('awr-latches')
        if not file_uploaded:
            df2tar(df_awr_latch, tar_file, 'awr-latches.csv')
        if df_awr_latch is not None:
            if is_cdb:
                df_awr_latch = df_awr_latch.merge(
                    df_pdb[['con_id', 'name']], on='con_id')
                _s = filter_pdb(df_awr_latch, 'name')
                df_awr_latch = df_awr_latch.loc[_s, ['name', 'instance_number', 'latch_name', 'begin_interval_time', 'wait_time', 'wait_time_delta', 'waits_var'
                                ]]
            else:
                df_awr_latch = df_awr_latch[['name', 'instance_number', 'latch_name', 'begin_interval_time', 'wait_time', 'wait_time_delta', 'waits_var'
                        ]]
            df_awr_latch['begin_interval_time'] = df_awr_latch['begin_interval_time'].apply(
                lambda x: datetime.datetime.strftime(x, '%m-%d %H:%M'))
            if st.checkbox('显示AWR Latch原始数据'):
                st.write(df_awr_latch.rename(columns=translate_word))
            # latch chart， 每个container一个单独的chart， instance_number+latch_name 决定series， begin_interval_date 作为x轴
            for con_name in df_awr_latch['name'].unique():
                # st.title(con_name + ' 历史Latch等待， 数据来源AWR')
                df_con = df_awr_latch.loc[df_awr_latch['name']
                                        == con_name].fillna(method='ffill')
                # Top-N gets
                var_ser = df_con.groupby(['instance_number', 'latch_name'])[
                    'waits_var'].mean().sort_values().tail(n=15)
                var_ser = var_ser.reset_index()
                top_n_stats = var_ser['latch_name'].tolist()
                
                option = {
                    'title': {
                        'text': con_name + ' Latch等待(AWR快照间隔内[默认1小时],单位 us)， 数据来源AWR'
                    },
                    'tooltip': {
                        'trigger': 'axis'
                    },
                    'legend': {
                        'bottom': 'bottom'
                    },
                    'grid': {
                        'left': '3%',
                        'right': '4%',
                        'bottom': '20%',
                        'containLabel': True
                    },
                    'toolbox': {
                        'feature': {
                            'saveAsImage': {}
                        }
                    },
                    'xAxis': {
                        'type': 'category',
                        'data': df_con['begin_interval_time'].unique().tolist()
                    },
                    'yAxis': {
                        'type': 'value'
                    },
                    'series': [
                        {
                            'name': _latch_name+'@'+str(_instance_number),
                            'data': grp_data['wait_time_delta'].fillna(0).values.tolist(),
                            'type': 'line',
                            'smooth': True
                        } for (_instance_number,_latch_name), grp_data in df_con.groupby(['instance_number','latch_name']) if _latch_name in top_n_stats
                            
                    ]
                }
                st_echarts(option, height=600)
        else:
            st.warning('数据未存储')

    # bg event awr, just like AWR Latch
    if st.checkbox('显示AWR中记录的后台(background)等待事件信息'):
        util_gen_awr_section('Background Event等待', 'awr-bg-event-wait',
                             'cdb_hist_bg_event_summary' if is_cdb else 'dba_hist_bg_event_summary', 'event_name', 'TIME_WAITED_MICRO', is_cdb, df_pdb, unit='us')
    
    if st.checkbox('显示AWR中记录的SGA内存信息'):
        util_gen_awr_section('SGA 组件信息', 'awr-sga-stat',
                             'cdb_hist_sgastat' if is_cdb else 'dba_hist_sgastat', 'name', 'bytes', is_cdb, df_pdb, unit='us')
    

    # bg event end.

    # time model awr, just like AWR Latch
    if st.checkbox('显示AWR中的时间模型(Time Model)数据'):
        util_gen_awr_section('Time Model等待', 'awr-sys-time-model',
                             'cdb_hist_con_sys_time_model' if is_cdb else 'dba_hist_con_sys_time_model', 'stat_name', 'value', is_cdb, df_pdb, unit='s%')
    

    # sys time model end.

    # miscellaneous Oracle Real Application Clusters (Oracle RAC) statistics in awr, just like AWR Latch
    if st.checkbox('显示AWR中的RAC Misc信息'):
        util_gen_awr_section('RAC MISC等待', 'awr-rac-misc',
                             'CDB_HIST_DLM_MISC' if is_cdb else 'DBA_HIST_DLM_MISC', 'name', 'value', is_cdb, df_pdb)
    

    # miscellaneous Oracle Real Application Clusters (Oracle RAC) statistics end.

    # enqueue in awr
    if st.checkbox('显示AWR中的Enqueue锁相关信息'):
        util_gen_awr_section('Enqueue等待', 'awr-enqueue-hist',
                             'CDB_HIST_ENQUEUE_STAT' if is_cdb else 'DBA_HIST_ENQUEUE_STAT', 'eq_type', 'CUM_WAIT_TIME', is_cdb, df_pdb)
    

    # enqueue end.

    # event histogram in awr
    if st.checkbox('显示AWR中的数据文件IO信息:'):
        util_gen_awr_section('数据文件IO', 'awr-file-statx',
                             'CDB_HIST_FILESTATXS' if is_cdb else 'DBA_HIST_FILESTATXS', 'filename', 'time', is_cdb, df_pdb)
    
    
    if st.checkbox('显示AWR中的操作系统统计信息历史:'):
        util_gen_awr_section('OS Statistics History', 'os-stat-hist',
                             'CDB_HIST_OSSTAT' if is_cdb else 'DBA_HIST_OSSTAT', 'stat_name', 'value', is_cdb, df_pdb)
    if st.checkbox('显示AWR中记录的SQL执行统计信息:'):
        util_gen_awr_section('SQL Statistics(执行时间) History', 'sql-stat-hist',
                         'cdb_hist_sqlstat', 'sql_id', 'ELAPSED_TIME_TOTAL', is_cdb, df_pdb)
    if st.checkbox('显示AWR中记录的数据库统计信息:'):
        util_gen_awr_section('System statistics History', 'sys-stat-hist',
                             'cdb_HIST_CON_SYSSTAT' if is_cdb else 'dba_HIST_CON_SYSSTAT', 'stat_name', 'value', is_cdb, df_pdb,
                         include_stats=['redo writes', 'consistent reads', 'db block changes', 
                            'physical reads', 'physical writes',  'user calls', 'parse count (total)', 
                            'parse count (hard)', 'Logons', 'user logons cumulative', 'user commits', 
                            'user rollbacks'])
    if st.checkbox('显示AWR中记录的非空闲等待事件数据:'):
        idle_events = query_event_class(
            st.session_state.selected_conn) if not file_uploaded else df_from_tar('idle-events')
        if not file_uploaded:
            df2tar(idle_events, tar_file, 'idle-events')

        idle_events = idle_events[idle_events['wait_class'] == 'Idle']
        util_gen_awr_section('System wait Event History', 'sys-event-hist',
                             'cdb_hist_con_system_event' if is_cdb else 'dba_hist_con_system_event', 'event_name', 'TIME_WAITED_MICRO', is_cdb, df_pdb, exclude_stats=idle_events['name'].values.tolist())

if show_jobs:
    st.header('数据库中Job信息')
    st.subheader('数据库自动维护任务(自动统计信息搜集等)')
    df_auto_tasks: pd.DataFrame = query_autotasks(
        st.session_state.selected_conn, is_cdb) if not file_uploaded else df_from_tar('db-auto-tasks')
    if not file_uploaded:
        df2tar(df_auto_tasks, tar_file, 'db-auto-tasks.csv')
    if df_auto_tasks is not None:
        if is_cdb:
            df_auto_tasks = df_auto_tasks.merge(
                df_pdb[['con_id', 'name']], on='con_id')
            _s = filter_pdb(df_auto_tasks, 'name')
            df_auto_tasks = df_auto_tasks.loc[_s, ['name', 'client_name', 'task_name',
                                                'operation_name', 'status', 'window_group_name', 'enabled', 'next_start_date']]
        else:
            df_auto_tasks = df_auto_tasks[['client_name', 'task_name', 'operation_name',
                    'status', 'window_group_name', 'enabled', 'next_start_date']]
        st.write(df_auto_tasks.rename(columns=translate_word))
    else:
        st.warning('数据未存储')

    st.subheader('Scheduler Jobs...')
    df_sched_jobs = query_sched_jobs(
        st.session_state.selected_conn, is_cdb) if not file_uploaded else df_from_tar('sched-jobs')
    if not file_uploaded:
        df2tar(df_sched_jobs, tar_file, 'sched-jobs.csv')
    if df_sched_jobs is not None:
        if is_cdb:
            df_sched_jobs = df_sched_jobs.merge(
                df_pdb[['con_id', 'name']], on='con_id')
            _s = filter_pdb(df_sched_jobs, 'name')
            df_sched_jobs = df_sched_jobs.loc[_s]
        else:
            df_sched_jobs = df_sched_jobs[['client_name', 'task_name', 'operation_name',
                    'status', 'window_group_name', 'enabled', 'next_start_date']]
        state_all = df_sched_jobs['state'].unique().tolist()
        # state_all
        select_job_state = st.selectbox(label='选择作业状态:',options=['ALL',] + state_all)
        if select_job_state and select_job_state != 'ALL':
            df_sched_jobs = df_sched_jobs.loc[df_sched_jobs['state']
                                            == select_job_state]
        st.write(df_sched_jobs.rename(columns=translate_word))
    else:
        st.warning('数据未存储')

tar_file.close()
if not file_uploaded:
    st.sidebar.download_button(
        label="下载已获取数据...",
        data=gen_tar_data,
        file_name=f"{st.session_state.selected_conn}-{datetime.datetime.strftime(datetime.datetime.now(), '%y-%m-%d_%H-%M')}-.tar.gz",
        mime='application/octet-stream',
    )
