# 通过SQL搜集数据，供后期上传分析

## 需要使用Oracle SQLcl 工具执行脚本，才能正常搜集数据

## 仅19c CDB环境进行过测试

## 由于SQLcl环境中， 当前文件夹会变成SQLcl可执行程序所在文件夹， 进入SQLcl后要通过cd命令切换当前文件夹
```
changzhenghe@Changzheng-Hes-MacBook-Pro bin % ./sql c##check@localhost/wsatdb

SQLcl: 发行版 19.4 Production, 发行日期 周四 3月 31 11:48:17 2022

版权所有 (c) 1982, 2022, Oracle。保留所有权利。

口令? (**********?) **********
Last Successful login time: 星期四 3月  31 2022 11:48:01 +08:00

已连接到:
Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production
Version 19.3.0.0.0


SQL> cd /Users/changzhenghe/github/orcl_diag/sqlcl-scripts
SQL> @ gather_cdb
```

## 用户权限(CDB环境的示例)
```
create user c##check identified by ***** default tablespace sysaux container=all;
grant connect,resource,oem_monitor to c##check container=all;
alter user c##check set container_data=all container=current;
```

## CDB 环境，连接到root cdb ,执行 gather_cdb.sql
执行前要确认当前文件夹中已经创建output文件夹，用于存储产生的数据，如果已有output文件夹，应考备份、清空

```
SQL> @ gather_cdb
```

## 非CDB 环境，连接后执行 gather_nocdb.sql
```
SQL> @ gather_nocdb
```

# 压缩、打包文件
```
$> cd <script_output_dir>
tar czvf <you_specified_filename>.tar.gz ./*.csv

```
生成的压缩包，后续可以上传系统，进行可视化查看。
