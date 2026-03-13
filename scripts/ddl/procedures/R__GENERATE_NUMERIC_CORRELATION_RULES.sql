USE DATABASE {{ snowflake_database }};

USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE PROCEDURE "GENERATE_NUMERIC_CORRELATION_RULES"("P_TABLE_NAME" VARCHAR, "P_COLUMNS" ARRAY, "P_THRESHOLD" FLOAT)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('pandas', 'numpy', 'snowflake-snowpark-python')
HANDLER = 'generate_correlation_rules_handler'
EXECUTE AS OWNER
AS '';
