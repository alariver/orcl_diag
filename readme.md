## 做什么的？
1. 通过用户在UI中提供的连接Oracle数据库的信息，连接到数据库，获取诊断信息，以表格、chart（主要使用baidu echarts）展示
2. 通过一些checkbox，让用户选择性的显示（查询）信息
3. 下载数据（将Oracle数据库中获取的信息，以CSV格式，打包为压缩的tarfile）
4. 上传、显示离线数据（在不连接Oracle数据库的情况下，将上述下载的离线数据重新展现）
5. **主要是便于运维人员日常查看Orcl数据库状态,以及Support工程师远程通过离线的数据压缩包诊断问题**

## 主要特点
1. read only方式访问数据库
2. python + streamlit + pandas 实现

## 为什么花时间做这个？
1. 觉得 streamlit 这个东西简单的实现了很强大的功能，通过这种方式，慢慢的把积累的各种Oracle 运维scripts，集中运行、展示
2. 希望介绍一种技能要求很低的，数据展示强大的平台，吸纳、集成其他工程师的经验 ———— 吸引更多的人扩展功能、提供MySQL的实现、提供PostgreSQL的实现……

## 基础技能介绍
### python 以及 pandas
要求都不高，使用pandas通过写SQL读取数据非常方便
```
import pandas as pd
df:pd.DataFrame = pd.read_sql(sql_text, connection)
# ... df 可以在streamlit中直接以表格形式展示

```
### streamlit
[streamlit](www.streamlit.io) 是以纯python方式生成web应用的超级简单的解决方案，不用为web前端费心。

### echarts
Baidu 的echarts可以实现各种图表的绘制， [echarts](echarts.apache.org)
### Oracle SQL
queryUtil.py 文件中提供了一些查询Oracle的SQL脚本，及时不懂python，也能读懂是做什么的。
SQL稍微复杂一点的就是使用了Oracle的分析函数（也有称窗口函数）

## 运行
附带的requirements.txt中，已经列出了依赖的python package
使用python 3.8
```
# 完成python 3.8 安装
# 新建一个virtualenv环境
python3 -m venv <venv_dir>
# 激活venv环境
source <venv_dir>/bin/activate
# 安装依赖的package(安装pandas、cx_Oracle 可能会遇到一些麻烦，借助搜索引擎，应不难解决)
pip install -r requirements.txt

# 运行
python streamlit run slt.py
# 或者
python streamlit_wrapper.py
# 根据提示，浏览器访问...

```
提供了一个streamlit_wrapper.spec 文件，用于使用pyinstaller 打包生成可执行文件（Windows、Macos）