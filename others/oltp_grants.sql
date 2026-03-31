-- 1. Configure the environment to output only the SQL commands
SET HEADING OFF;
SET FEEDBACK OFF;
SET PAGESIZE 0;
SET TRIMSPOOL ON;
SET LINESIZE 200;

-- 2. Define the output file name
SPOOL oltp_grants.sql

-- 3. Generate the GRANT statements for all tables in the OLTP schema
SELECT 'GRANT ALL ON OLTP.' || table_name || ' TO amit;'
FROM all_tables
WHERE owner = 'OLTP';

-- 4. Close the file
SPOOL OFF;
