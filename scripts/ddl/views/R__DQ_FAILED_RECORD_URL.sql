USE DATABASE {{ snowflake_database }};

USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE VIEW DQ_FAILED_RECORD_URL (
    DATASET_NAME,
    DATASET_RUN_ID,
    CREATED_TIMESTAMP,
    FAILURE_TABLE,
    FAILED_FILE_URL
) AS (
    WITH RankedRuns AS (
        SELECT
            d.dataset_name,
            r.dataset_run_id,
            GET_PRESIGNED_URL('@DQ_FAILURES_STAGE', r.FAILURE_FILE, 604800) AS failed_file_url,
            -- r.failed_file_url,
            r.created_timestamp,
            res.results:failed_records_table::STRING AS failure_table,
            ROW_NUMBER() OVER (
                PARTITION BY d.dataset_name
                ORDER BY r.created_timestamp DESC
            ) AS run_rank
        FROM dq_dataset_run_log r
        JOIN dq_dataset d ON d.dataset_id = r.dataset_id
        LEFT JOIN dq_rule_results res ON r.dataset_run_id = res.dataset_run_id
        WHERE
            res.is_success = false
            AND res.results:failed_records_table::STRING LIKE '%_DQ_FAILURE'
    )
    SELECT
        dataset_name,
        dataset_run_id,
        created_timestamp,
        failure_table,
        failed_file_url
    FROM RankedRuns
    WHERE run_rank = 1
    ORDER BY created_timestamp DESC
);
