import streamlit.cli as stcli
import sys,os
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

# datas=[('/Users/changzhenghe/github/orcl_diag/lib/python3.8/site-packages/altair/','./altair/'),('/Users/changzhenghe/github/orcl_diag/slt.py','.'),('/Users/changzhenghe/github/orcl_diag/queryUtil.py','.')],

def streamlit_run():
    # this_dir = Path(__file__).parent
    # os.chdir(this_dir)
    # sys.path.append(this_dir.name)
    sys.argv = ["streamlit", "run", "slt.py",
                "--global.developmentMode=false"]
    sys.exit(stcli.main())


if __name__ == '__main__':
    streamlit_run()
