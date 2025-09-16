SET DB_NAME = 'dev_payments';
SET sh_stage = 'STAGING';
SET Sh_raw = 'raw';
SET sh_mart = 'mart';
CREATE DATABASE IF NOT EXISTS IDENTIFIER($DB_NAME);
--CREATE SCHEMA  IF NOT EXISTS dev_payments.raw;
--CREATE SCHEMA  IF NOT EXISTS dev_payments.mart;


USE DATABASE IDENTIFIER($DB_NAME);
USE SCHEMA IDENTIFIER($Sh_raw);

// storage integration:

CREATE OR REPLACE STORAGE INTEGRATION s3_int_mindtree
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::481665128591:role/alerts_sf_project_role'
STORAGE_ALLOWED_LOCATIONS = 
(
  's3://mindtreedev/customers/',
  's3://mindtreepreprod/customers/',
  's3://mindtreeprod/customers/',
  's3://mindtreedev/payments/'
  
)
COMMENT = 'This is s3_int for all environments';


// CREATE FILE FORMAT:

CREATE OR REPLACE FILE FORMAT ff
TYPE  = 'CSV'
FIELD_DELIMITER = ','
FIELD_OPTIONALLY_ENCLOSED_BY ='"'
EMPTY_FIELD_AS_NULL = TRUE
DATE_FORMAT = 'YYYY-MM-DD'
SKIP_HEADER = 1;

// CREATION OF STAGE:

CREATE OR REPLACE STAGE stg_raw
URL = 's3://mindtreedev/payments/'
STORAGE_INTEGRATION = s3_int_mindtree
FILE_FORMAT  = ff;


// CREATION OF TABLE SCHEMA:


CREATE OR REPLACE TABLE payments_raw
(
    payment_id   STRING,
    user_id      STRING,
    amount       NUMBER(10,2),
    currency     STRING,
    payment_ts   TIMESTAMP_NTZ,
    file_name VARCHAR,
    file_row_number INT,
    INGESTION_TS TIMESTAMP_NTZ 
    --DEFAULT current_timestamp
    
);

// STREAM CREATION 

CREATE OR REPLACE STREAM PAYMENT_RAW_STREAM
ON TABLE payments_raw;

// PIPE CREATION:

CREATE OR REPLACE PIPE s3_pipe
AUTO_INGEST = TRUE
AS
COPY INTO payments_raw
FROM 
(
    SELECT $1,$2,$3,$4,$5,METADATA$FILENAME,METADATA$FILE_ROW_NUMBER,TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
    FROM @stg_raw
)
FILE_FORMAT = ff
PATTERN = '.*payments.*';