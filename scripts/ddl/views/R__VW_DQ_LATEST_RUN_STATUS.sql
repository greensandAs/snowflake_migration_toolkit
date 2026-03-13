USE DATABASE {{ snowflake_database }};

USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE VIEW VW_DQ_LATEST_RUN_STATUS (
    RULE_CONFIG_ID,
    DATASET_RUN_ID,
    DATASET_NAME,
    DIMENSION,
    IS_SUCCESS,
    RUN_TIMESTAMP,
    RUN_DATE
) AS
SELECT
    C.RULE_CONFIG_ID,
    R.DATASET_RUN_ID,
    D.DATASET_NAME,
    COALESCE(NULLIF(C.DIMENSION, ''), M.DIMENSION) AS DIMENSION,
    R.IS_SUCCESS,
    R.run_timestamp,
    TO_DATE(R.run_timestamp) AS RUN_DATE
FROM DQ_RULE_RESULTS R
JOIN DQ_RULE_CONFIG C

    ON
        R.rule_config_id = C.rule_config_id
        AND C.is_active = 'Y'
JOIN DQ_EXPECTATION_MASTER M
    ON M.expectation_id = C.expectation_id
JOIN DQ_DATASET D
    ON D.DATASET_ID = C.DATASET_ID
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY C.RULE_CONFIG_ID
        ORDER BY R.run_timestamp DESC
    ) = 1
ORDER BY R.run_timestamp DESC;
