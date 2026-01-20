USE DATABASE {{ snowflake_database }};
USE SCHEMA {{ snowflake_schema }};
ALTER TABLE {{ snowflake_database }}.{{ snowflake_schema }}.DQ_DATASET
RENAME COLUMN SELECTED_COULMNS TO SELECTED_COLUMNS;
