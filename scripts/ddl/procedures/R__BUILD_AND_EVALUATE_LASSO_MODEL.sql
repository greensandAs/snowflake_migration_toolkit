USE DATABASE {{ snowflake_database }};

USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE PROCEDURE "BUILD_AND_EVALUATE_LASSO_MODEL"(
    "P_TABLE_NAME" VARCHAR,
    "P_FEATURES" ARRAY,
    "P_TARGET" VARCHAR,
    "P_RELATIONS" ARRAY,
    "P_DATA_CONFIG" VARCHAR,
    "P_COMPUTED_VAR" VARCHAR,
    "P_CODE" VARCHAR,
    "P_REL" VARCHAR
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('pandas', 'numpy', 'scikit-learn', 'snowflake-snowpark-python')
HANDLER = 'build_lasso_with_relations_handler'
EXECUTE AS OWNER
AS '';
