USE DATABASE {{ snowflake_database }};

USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE VIEW VW_DATASET_NM (
    DATASET_ID,
    PROJECT_ID,
    PROJECT_NAME,
    DATASET_NAME,
    DATABASE_NAME,
    SCHEMA_NAME,
    TABLE_NAME,
    CUSTOM_SQL
) AS
SELECT
    dataset_id,
    project_id,
    UPPER(project_name) AS project_name,
    UPPER(dataset_name) AS dataset_name,
    database_name,
    schema_name,
    table_name,
    custom_sql
FROM DQ_DATASET;
