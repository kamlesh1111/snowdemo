-- this is for cf account load process...
-- we will be using existing file format and account intg.. 

-- File Format: CSV_SALE_DATA
-- Storage INTG: EXT_AWS_SALES_STAGE

use SCHEMA DEV_LAND_SCH;

CREATE OR REPLACE PROCEDURE SP_LOAD_CF_ACCT_DATA(db_name varchar,sch_name varchar)
 RETURNS boolean
 AS
    DECLARE
        sp_status boolean default TRUE;
        copy_stmt varchar default '';
        insert_stmt varchar default '';
        line varchar default '';
        my_exception EXCEPTION (-20003, 'Raised CF-ACCT_LOAD EXCEPTION LANDING ZONE ACCT_DATA LOAD .');
    BEGIN
        sp_status := true;
        copy_stmt := 'COPY INTO '||db_name||'.'||sch_name||'.'||' TBL_CF_ACCT
        FROM (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12, 
                    METADATA$FILENAME,CURRENT_TIMESTAMP() FROM @EXT_AWS_SALES_STAGE/CF_INP_FEED) ON_ERROR = CONTINUE';
        
        EXECUTE IMMEDIATE :copy_stmt;
                            
        RETURN sp_status;

        EXCEPTION
            WHEN statement_error or EXPRESSION_ERROR THEN
                sp_status := FALSE;
                LET LINE := 'STMT OR EXPR ERROR,'||SQLCODE || ': ' || SQLERRM;
                INSERT INTO EXCEPTION_DATA VALUES (:line,CURRENT_TIMESTAMP());
                -- HERE WE ARE NOT RAISING EXCEPTION HENCE CODE WILL CONTINUE
                RAISE my_exception; 
                              
            WHEN OTHER THEN
                sp_status := FALSE;
                LET LINE := 'OTHER ERROR,'||SQLCODE || ': ' || SQLERRM;
                INSERT INTO EXCEPTION_DATA VALUES (:line,CURRENT_TIMESTAMP());
                RAISE my_exception; 
    END;

-- use SCHEMA DEV_LAND_SCH;

USE SCHEMA DEV_CUR_SCH;

CREATE OR REPLACE PROCEDURE SP_CURZ_LOAD_CF_ACCT_DATA(db_name varchar,lzone_sch_name varchar,czone_sch_name varchar)
 RETURNS boolean
 AS
    DECLARE
        sp_status boolean default TRUE;
        insert_cfact_stmt varchar default '';
        line varchar default '';
        my_exception EXCEPTION (-20013, 'Raised CF-ACCT_LOAD EXCEPTION CURRATED ZONE ACCT_DATA LOAD .');
    BEGIN
        sp_status := true;
        insert_cfact_stmt := 'INSERT INTO '||db_name||'.'||czone_sch_name||'.'||'TBL_CF_ACCT(RECORD_TYPE, 
        CARD_NUM, ACCT_NUM, F_NAME, L_NAME, EMAIL, GENDER, EFFECTIVE_DT, END_DT, ADDRESS, 
        PHONE, DOB, FL_NAME, LOAD_TS)  
        SELECT RECORD_TYPE, 
        CARD_NUM, ACCT_NUM, F_NAME, L_NAME, EMAIL, GENDER, 
        TO_CHAR((TRY_TO_DATE(EFFECTIVE_DT,''DD/MM/YYYY'')),''YYYY-MM-DD''), 
        TO_CHAR((TRY_TO_DATE(END_DT,''DD/MM/YYYY'')),''YYYY-MM-DD''),
        ADDRESS, 
        REPLACE(PHONE,''-''), 
        TO_CHAR((TRY_TO_DATE(DOB,''DD/MM/YYYY'')),''YYYY-MM-DD''), 
        FL_NAME, 
        LOAD_TS 
        FROM '||db_name||'.'||lzone_sch_name||'.'||'strm_landz_cf_acc';
        
        EXECUTE IMMEDIATE :insert_cfact_stmt;
                            
        RETURN sp_status;

        EXCEPTION
            WHEN statement_error or EXPRESSION_ERROR THEN
                sp_status := FALSE;
                LET LINE := 'STMT OR EXPR ERROR,'||SQLCODE || ': ' || SQLERRM;
                INSERT INTO EXCEPTION_DATA VALUES (:line,CURRENT_TIMESTAMP());
                -- HERE WE ARE NOT RAISING EXCEPTION HENCE CODE WILL CONTINUE
                RAISE my_exception; 
                              
            WHEN OTHER THEN
                sp_status := FALSE;
                LET LINE := 'OTHER ERROR,'||SQLCODE || ': ' || SQLERRM;
                INSERT INTO EXCEPTION_DATA VALUES (:line,CURRENT_TIMESTAMP());
                RAISE my_exception; 
    END;

USE SCHEMA DEV_CONLAYER_SCH;

CREATE OR REPLACE PROCEDURE SP_CONS_LOAD_CF_ACCT_DATA()
 RETURNS boolean
 AS
    DECLARE
        sp_status boolean default TRUE;
        line varchar default '';
        my_exception EXCEPTION (-20023, 'Raised CF-ACCT_LOAD EXCEPTION CONSUMPTION LAYER ACCT_DATA LOAD .');
    BEGIN
        sp_status := true;

        MERGE INTO DB_DEV.DEV_CONLAYER_SCH.TBL_CF_ACCT CL
        USING (SELECT RECORD_TYPE, CARD_NUM, ACCT_NUM, 
                    INITCAP(F_NAME) AS F_NAME ,INITCAP(L_NAME) AS L_NAME,
                    EMAIL, CASE WHEN UPPER(GENDER) = 'MALE' THEN 'MALE'
                                    WHEN UPPER(GENDER) = 'FEMALE' THEN 'FEMALE'
                                    ELSE 'OTHER'
                                    END AS GENDER,
                    NVL(EFFECTIVE_DT,CURRENT_DATE) AS EFFECTIVE_DT,
                    NVL(END_DT,'9999-12-31') AS END_DT,
                    INITCAP(ADDRESS) AS ADDRESS,
                    PHONE,
                    NVL(DOB,'1900-01-01') AS DOB
                FROM DB_DEV.DEV_CUR_SCH.STRM_CURZ_CF_ACC 
                WHERE NOT (METADATA$ACTION = 'DELETE' AND METADATA$ISUPDATE = TRUE)) CRZ
            ON (CL.CARD_NUM = CRZ.CARD_NUM)
        
        WHEN MATCHED THEN UPDATE SET CL.END_DT = CURRENT_DATE()

        WHEN NOT MATCHED THEN 
            INSERT (CL.RECORD_TYPE, CL.CARD_NUM, CL.ACCT_NUM, CL.F_NAME, CL.L_NAME, CL.EMAIL, 
                    CL.GENDER, CL.EFFECTIVE_DT, CL.END_DT, CL.ADDRESS, CL.PHONE, CL.DOB, CL.LOAD_TS)
            VALUES (CRZ.RECORD_TYPE, CRZ.CARD_NUM, CRZ.ACCT_NUM, CRZ.F_NAME, CRZ.L_NAME, CRZ.EMAIL, 
                    CRZ.GENDER, CRZ.EFFECTIVE_DT, CRZ.END_DT, CRZ.ADDRESS, CRZ.PHONE, CRZ.DOB, CURRENT_TIMESTAMP());
                            
        RETURN sp_status;

        EXCEPTION
            WHEN statement_error or EXPRESSION_ERROR THEN
                sp_status := FALSE;
                LET LINE := 'STMT OR EXPR ERROR,'||SQLCODE || ': ' || SQLERRM;
                INSERT INTO EXCEPTION_DATA VALUES (:line,CURRENT_TIMESTAMP());
                -- HERE WE ARE NOT RAISING EXCEPTION HENCE CODE WILL CONTINUE
                RAISE my_exception; 
                              
            WHEN OTHER THEN
                sp_status := FALSE;
                LET LINE := 'OTHER ERROR,'||SQLCODE || ': ' || SQLERRM;
                INSERT INTO EXCEPTION_DATA VALUES (:line,CURRENT_TIMESTAMP());
                RAISE my_exception; 
    END;

