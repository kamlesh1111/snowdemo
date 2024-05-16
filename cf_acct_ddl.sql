-- this is ddl for carte france account maintenance 

USE DATABASE db_dev;

-- this for landing zone tables
use SCHEMA DEV_LAND_SCH;

CREATE TRANSIENT TABLE IF NOT EXISTS TBL_CF_ACCT  (RECORD_TYPE VARCHAR,
                                    CARD_NUM VARCHAR,
                                    ACCT_NUM VARCHAR,
                                    F_NAME VARCHAR,
                                    L_NAME VARCHAR,
                                    EMAIL VARCHAR,
                                    GENDER VARCHAR,
                                    EFFECTIVE_DT VARCHAR,
                                    END_DT VARCHAR,
                                    ADDRESS VARCHAR,
                                    PHONE VARCHAR,
                                    DOB VARCHAR,
                                    FL_NAME VARCHAR,
                                    LOAD_TS VARCHAR
                                    ) COMMENT = 'THIS IS ACCOUNT TABLE';

DESC TABLE TBL_CF_ACCT;

commit;

show stages;

list @EXT_AWS_SALES_STAGE;

SHOW TABLES;

-- CREATE STREAM ON TABLE TBL_CF_ACC for data load to next stage. 

CREATE OR REPLACE STREAM STRM_LANDZ_CF_ACC ON TABLE TBL_CF_ACCT
APPEND_ONLY = TRUE;


-- Moving to Curatwed Zone DDL:

USE SCHEMA DEV_CUR_SCH;

CREATE TRANSIENT TABLE IF NOT EXISTS TBL_CF_ACCT  (RECORD_TYPE VARCHAR(10) NOT NULL,
                                    CARD_NUM NUMBER(16),
                                    ACCT_NUM VARCHAR(36),
                                    F_NAME VARCHAR(30),
                                    L_NAME VARCHAR(30),
                                    EMAIL VARCHAR(100),
                                    GENDER VARCHAR(15),
                                    EFFECTIVE_DT DATE,
                                    END_DT DATE,
                                    ADDRESS VARCHAR(150),
                                    PHONE NUMBER(15),
                                    DOB VARCHAR(10),
                                    FL_NAME VARCHAR(100),
                                    LOAD_TS TIMESTAMP
                                    ) COMMENT = 'THIS IS ACCOUNT TABLE';


-- CREATE STREAM ON CURRATED ZONE TABLE 

CREATE STREAM IF NOT EXISTS STRM_CURZ_CF_ACC ON TABLE TBL_CF_ACCT;

USE SCHEMA DEV_CONLAYER_SCH;

CREATE TABLE IF NOT EXISTS TBL_CF_ACCT  (RECORD_TYPE VARCHAR(10) NOT NULL,
                                    CARD_NUM NUMBER(16),
                                    ACCT_NUM VARCHAR(36),
                                    F_NAME VARCHAR(30),
                                    L_NAME VARCHAR(30),
                                    EMAIL VARCHAR(100),
                                    GENDER VARCHAR(15),
                                    EFFECTIVE_DT DATE,
                                    END_DT DATE,
                                    ADDRESS VARCHAR(150),
                                    PHONE NUMBER(15),
                                    DOB VARCHAR(10),
                                    LOAD_TS TIMESTAMP
                                    ) COMMENT = 'THIS IS ACCOUNT TABLE';




