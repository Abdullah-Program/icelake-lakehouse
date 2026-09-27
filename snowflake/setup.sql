-- ====================================================================
-- IceLake Local: Snowflake Multi-Engine Iceberg Setup & RBAC
-- Role: AccountAdmin / SysAdmin
-- ====================================================================

USE ROLE ACCOUNTADMIN;
CREATE WAREHOUSE IF NOT EXISTS ICELAKE_WH
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS ICELAKE_DB;
CREATE SCHEMA IF NOT EXISTS ICELAKE_DB.WIKIPEDIA;
USE SCHEMA ICELAKE_DB.WIKIPEDIA;

-- ====================================================================
-- OPTION 1: Snowflake-Managed Iceberg Table (Local Demo & Learning)
-- Use this if your Garage S3 is local (Snowflake Cloud cannot reach localhost)
-- ====================================================================

-- 1. Create External Volume (In AWS Free Trial, point to your S3 bucket)
-- CREATE OR REPLACE EXTERNAL VOLUME icelake_s3_vol
--    STORAGE_LOCATIONS =
--       (
--          (
--             NAME = 'my-s3-us-east-1'
--             STORAGE_PROVIDER = 'S3'
--             STORAGE_BASE_URL = 's3://my-cloud-iceberg-bucket/'
--             STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake_role'
--          )
--       );

-- 2. Create Snowflake Iceberg Table (Matching our Wikipedia schema)
CREATE OR REPLACE ICEBERG TABLE wikipedia_articles (
    id           STRING,
    title        STRING,
    text         STRING,
    url          STRING,
    created_date DATE,
    word_count   INT,
    category     STRING
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'iceberg_external_volume'
BASE_LOCATION = 'articles/';

-- 3. Querying the Iceberg Table in Snowflake
SELECT 
    YEAR(created_date) AS article_year,
    COUNT(*) AS total_articles,
    AVG(word_count) AS avg_words
FROM wikipedia_articles
GROUP BY 1
ORDER BY 1;

-- ====================================================================
-- OPTION 2: External Iceberg Table with Polaris REST Catalog
-- (When Polaris and S3 are hosted on Cloud / Cloudflare R2 / AWS)
-- ====================================================================

/*
CREATE OR REPLACE CATALOG INTEGRATION polaris_catalog_int
    CATALOG_SOURCE = ICEBERG_REST
    TABLE_FORMAT = ICEBERG
    CATALOG_NAMESPACE = 'wikipedia'
    REST_CONFIG = (
        CATALOG_URI = 'https://your-polaris-domain.com/api/catalog'
        WAREHOUSE = 'icelake'
    )
    REST_AUTHENTICATION = (
        TYPE = OAUTH
        OAUTH_CLIENT_ID = 'polaris'
        OAUTH_CLIENT_SECRET = 'polaris'
        OAUTH_ALLOWED_SCOPES = ('PRINCIPAL_ROLE:ALL')
    )
    ENABLED = TRUE;

-- Query the table synced directly from Polaris
CREATE OR REPLACE ICEBERG TABLE polaris_wikipedia_articles
    EXTERNAL_VOLUME = 'icelake_s3_vol'
    CATALOG = 'polaris_catalog_int'
    CATALOG_TABLE_NAME = 'articles';
*/

-- ====================================================================
-- PHASE 8: Snowflake Role-Based Access Control (RBAC)
-- ====================================================================

-- 1. Create Functional Roles
CREATE ROLE IF NOT EXISTS data_engineer;
CREATE ROLE IF NOT EXISTS data_analyst;
CREATE ROLE IF NOT EXISTS read_only_user;

-- 2. Establish Role Hierarchy
GRANT ROLE read_only_user TO ROLE data_analyst;
GRANT ROLE data_analyst TO ROLE data_engineer;
GRANT ROLE data_engineer TO ROLE SYSADMIN;

-- 3. Grant Permissions
-- Read-Only User can only query the table
GRANT USAGE ON WAREHOUSE ICELAKE_WH TO ROLE read_only_user;
GRANT USAGE ON DATABASE ICELAKE_DB TO ROLE read_only_user;
GRANT USAGE ON SCHEMA ICELAKE_DB.WIKIPEDIA TO ROLE read_only_user;
GRANT SELECT ON TABLE ICELAKE_DB.WIKIPEDIA.wikipedia_articles TO ROLE read_only_user;

-- Data Analyst can also perform DML
GRANT INSERT, UPDATE, DELETE ON TABLE ICELAKE_DB.WIKIPEDIA.wikipedia_articles TO ROLE data_analyst;

-- Data Engineer has full control
GRANT ALL ON SCHEMA ICELAKE_DB.WIKIPEDIA TO ROLE data_engineer;
