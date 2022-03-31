SET SQLFORMAT csv 
SPOOL 'output/undo-tablespace-usage.csv';
SELECT   t.con_id,t.tablespace_name
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
            GROUP BY t.con_id, t.tablespace_name;
SPOOL off;
SET SQLFORMAT ansiconsole