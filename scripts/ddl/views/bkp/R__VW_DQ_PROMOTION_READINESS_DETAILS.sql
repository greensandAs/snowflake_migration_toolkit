USE DATABASE {{ snowflake_database }};

USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE VIEW VW_DQ_PROMOTION_READINESS_DETAILS (
    DATASET_RUN_ID,
    RUN_DATE,
    DATASET_ID,
    DATASET_NAME,
    RUN_STAGE,
    RULE_CONFIG_ID,
    RULE_DESCRIPTION,
    COLUMN_NAME,
    SEVERITY,
    DIMENSION,
    SEVERITY_WEIGHT,
    IS_SUCCESS,
    TOTALRECORDS,
    PASSEDRECORDS,
    FAILEDRECORDS,
    PASSED_PCT,
    FAILED_PCT,
    CONTRIBUTIONFACTOR,
    RULECONTRIBUTION
) AS
SELECT
    DATASET_RUN_ID
    , RUN_DATE
    , DATASET_ID
    , DATASET_NAME
    , RUN_STAGE
    , RULE_CONFIG_ID
    , RULE_DESCRIPTION
    , COLUMN_NAME
    , SEVERITY
    , DIMENSION
    , SEVERITY_WEIGHT
    , IS_SUCCESS
    , TOTALRECORDS
    , PASSEDRECORDS
    , FAILEDRECORDS
    , PASSED_PCT
    , FAILED_PCT
    , CONTRIBUTIONFACTOR
    , RULECONTRIBUTION
FROM (
    SELECT
        RES.DATASET_RUN_ID
        , RES.RUN_TIMESTAMP::DATE AS RUN_DATE
        , RES.DATASET_ID
        , RES.DATASET_NAME
        , DENSE_RANK() OVER (PARTITION BY RES.DATASET_ID ORDER BY RES.DATASET_RUN_ID DESC) AS RECENT_RUNS
        , 'STAGE_' || RECENT_RUNS AS RUN_STAGE
        , RES.RULE_CONFIG_ID
        , CFG.RULE_DESCRIPTION
        , CFG.COLUMN_NAME
        , CFG.SEVERITY
        , CASE
            WHEN CFG.SEVERITY = 'High' THEN 3
            WHEN CFG.SEVERITY = 'Medium' THEN 2
            WHEN CFG.SEVERITY = 'Low' THEN 1
            ELSE 1
        END AS SEVERITY_WEIGHT
        , IFNULL(CFG.DIMENSION, 'SQL') AS DIMENSION
        , RES.ELEMENT_COUNT AS TotalRecords
        , (RES.ELEMENT_COUNT - RES.UNEXPECTED_COUNT) AS PassedRecords
        , RES.UNEXPECTED_COUNT AS FailedRecords
        , RES.IS_SUCCESS
        , ROUND((PassedRecords / TotalRecords) * 100, 2) AS Passed_Pct
        , ROUND((FailedRecords / TotalRecords) * 100, 2) AS Failed_Pct
        , CASE
            WHEN Failed_Pct = 0 THEN 1.00
            WHEN Failed_Pct BETWEEN 0.10 AND 5.09 THEN 0.95
            WHEN Failed_Pct BETWEEN 5.10 AND 10.09 THEN 0.90
            WHEN Failed_Pct BETWEEN 10.10 AND 15.09 THEN 0.85
            WHEN Failed_Pct BETWEEN 15.10 AND 20.09 THEN 0.80
            WHEN Failed_Pct BETWEEN 20.10 AND 30.09 THEN 0.70
            WHEN Failed_Pct BETWEEN 30.10 AND 50.09 THEN 0.50
            WHEN Failed_Pct BETWEEN 50.10 AND 75.09 THEN 0.20
            WHEN Failed_Pct BETWEEN 75.10 AND 100.00 THEN 0.10
            ELSE NULL
        END AS ContributionFactor
        -- ,CASE
        --     WHEN Failed_Pct = 0 THEN 1.00
        --     WHEN Failed_Pct > 0   AND Failed_Pct <= 5   THEN 0.95
        --     WHEN Failed_Pct > 5   AND Failed_Pct <= 10  THEN 0.90
        --     WHEN Failed_Pct > 10  AND Failed_Pct <= 15  THEN 0.85
        --     WHEN Failed_Pct > 15  AND Failed_Pct <= 20  THEN 0.80
        --     WHEN Failed_Pct > 20  AND Failed_Pct <= 30  THEN 0.70
        --     WHEN Failed_Pct > 30  AND Failed_Pct <= 50  THEN 0.50
        --     WHEN Failed_Pct > 50  AND Failed_Pct <= 75  THEN 0.20
        --     WHEN Failed_Pct > 75  AND Failed_Pct <= 100 THEN 0.10
        --     ELSE NULL
        -- END AS ContributionFactor
        , ROUND((SEVERITY_WEIGHT * (Passed_Pct / 100) * ContributionFactor), 2) AS RuleContribution
    FROM dq_rule_results AS res
    JOIN dq_rule_config AS cfg
        USING (rule_config_id)
    WHERE TRUE
    QUALIFY RECENT_RUNS <= 3
) AS PR_DETAILS
ORDER BY DATASET_RUN_ID DESC, RULE_CONFIG_ID;
