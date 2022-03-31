SET SQLFORMAT csv 
SPOOL 'output/tempfile-iostat.csv';
select name file_name, PHYRDS, PHYWRTS,SINGLEBLKRDS,READTIM,WRITETIM,
			SINGLEBLKRDTIM,AVGIOTIM,LSTIOTIM,MINIOTIM,MAXIORTM,MAXIOWTM 
			from v$tempfile d,v$tempstat t
			where d.file# = t.file#;
SPOOL off;
SET SQLFORMAT ansiconsole