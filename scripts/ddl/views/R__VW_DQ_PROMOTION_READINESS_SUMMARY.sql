USE DATABASE {{ snowflake_database }};

USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE VIEW VW_DQ_PROMOTION_READINESS_SUMMARY (
    RUN_DATE,
    DATASET_RUN_ID,
    DATASET_ID,
    DATASET_NAME,
    RUN_STAGE,
    SUM_RULE_CONTRIBUTION,
    SUM_SEVERITY_WEIGHT,
    DQSCORE
) AS
SELECT
    run_date
    , dataset_run_id
    , dataset_id

    , dataset_name
    , run_stage
    , sum(rulecontribution) sum_rule_contribution
    , sum(severity_weight) sum_severity_weight
    , round((sum_rule_contribution / sum_severity_weight) * 100, 2) AS DQScore
FROM VW_DQ_PROMOTION_READINESS_DETAILS
GROUP BY ALL
ORDER BY DATASET_NAME, RUN_STAGE ASC;
