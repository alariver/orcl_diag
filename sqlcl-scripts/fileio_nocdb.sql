SET SQLFORMAT csv 
SPOOL 'output/dbfile-iostat.csv';
select name file_name, PHYRDS, PHYWRTS,SINGLEBLKRDS,READTIM,WRITETIM,
			SINGLEBLKRDTIM,AVGIOTIM,LSTIOTIM,MINIOTIM,MAXIORTM,MAXIOWTM 
			from v$datafile d,v$filestat t
			where d.file# = t.file#;
SPOOL off;
SET SQLFORMAT ansiconsole