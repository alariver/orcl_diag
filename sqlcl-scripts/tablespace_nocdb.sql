-- NON CDB
SET SQLFORMAT csv 
SPOOL 'output/tablespace-usage.csv';
SELECT   t.tablespace_name
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
            GROUP BY t.tablespace_name;
SPOOL off;
SET SQLFORMAT ansiconsole