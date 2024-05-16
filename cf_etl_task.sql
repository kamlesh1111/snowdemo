-- this has all tasks created for Carte France projects.. 
USE DATABASE DB_DEV;

-- refer schema as per etl flow
use schema dev_land_sch;

CREATE TASK IF NOT EXISTS JOBCF_ACCT_LOADA
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = 'USING CRON * 7 * * * UTC'
AS  
    CALL SP_LOAD_CF_ACCT_DATA('DB_DEV','DEV_LAND_SCH');

USE SCHEMA DEV_CUR_SCH;

CREATE TASK IF NOT EXISTS JOBCF_ACCT_LOADB
-- AFTER JOBCF_ACCT_LOADA -> can't do it as task are from different schema's
SCHEDULE = 'USING CRON 30 7 * * * UTC'
WHEN SYSTEM$STREAM_HAS_DATA('STRM_LANDZ_CF_ACC')
AS 
    call SP_CURZ_LOAD_CF_ACCT_DATA('DB_DEV','DEV_LAND_SCH','DEV_CUR_SCH');


USE SCHEMA DEV_CONLAYER_SCH;

CREATE TASK IF NOT EXISTS JOBCF_ACCT_LOADC
-- AFTER JOBCF_ACCT_LOADA -> can't do it as task are from different schema's
SCHEDULE = 'USING CRON * 8 * * * UTC'
WHEN SYSTEM$STREAM_HAS_DATA('STRM_CURZ_CF_ACC')
AS 
    call SP_CONS_LOAD_CF_ACCT_DATA();

