USE DATABASE {{ snowflake_database }};

USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE VIEW VW_DQ_RULE_CONFIG_NM (
    RULE_CONFIG_ID,
    CHECK_TYPE,
    DIMENSION
) AS
SELECT
    RULE_CONFIG_ID,
    -- 'en-ci' makes the column English Case-Insensitive
    CHECK_TYPE COLLATE 'en-ci' AS CHECK_TYPE,
    DIMENSION COLLATE 'en-ci' AS DIMENSION
FROM DQ_RULE_CONFIG;
