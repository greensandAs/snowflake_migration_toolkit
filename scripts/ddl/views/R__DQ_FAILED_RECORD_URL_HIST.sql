USE DATABASE {{ snowflake_database }};

USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE VIEW DQ_FAILED_RECORD_URL_HIST (
    DATASET_ID,
    DATASET_NAME,
    DATASET_RUN_ID,
    CREATED_TIMESTAMP,
    FAILURE_TABLE,
    FAILURE_FILE,
    FAILED_FILE_URL
) AS
SELECT
    d.dataset_id,
    d.dataset_name,
    r.dataset_run_id,

    r.created_timestamp,
    res.results:failed_records_table::STRING AS failure_table,
    r.failure_file,
    GET_PRESIGNED_URL('@DQ_FAILURES_STAGE', r.FAILURE_FILE, 604800) AS failed_file_url
FROM dq_dataset_run_log r
JOIN dq_dataset d
    ON d.dataset_id = r.dataset_id
JOIN dq_rule_results res
    ON r.dataset_run_id = res.dataset_run_id
WHERE
    res.is_success = false
    AND r.failure_file IS NOT NULL
    -- AND R.DATASET_ID = 252
    AND ENDSWITH(res.results:failed_records_table::STRING, '_DQ_FAILURE')
GROUP BY ALL
ORDER BY d.dataset_name, r.dataset_run_id DESC;
