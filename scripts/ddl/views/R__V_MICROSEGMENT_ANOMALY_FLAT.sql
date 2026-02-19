USE DATABASE {{ snowflake_database }};

USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE VIEW V_MICROSEGMENT_ANOMALY_FLAT (
    DATASET_ID,
    KPI_ID,
    MICROSEGMENT_DEF_ID,
    MICROSEGMENT_ID,
    MICROSEGMENT_PROFILER_SNAPSHOT_ID,
    MICROSEGMENT_DEF_NAME,
    MICROSEGMENT_DEF_DESCRIPTION,
    EXECUTION_START_TS,
    EXECUTION_END_TS,
    ATTRIBUTE_NAME,
    ATTRIBUTE_VALUE,
    METRIC_NAME,
    METRIC_VALUE,
    TIME_ATTRIBUTE_NAME,
    TIME_ATTRIBUTE_VALUE,
    ANOMALY_SCOPE,
    ANOMALY_DETECTION_METHOD,
    ANOMALY_REASON,
    CONFIDENCE_SCORE,
    ANOMALY_CREATED_TS
) AS
WITH base AS (
    SELECT
        md.dataset_id,
        md.kpi_id,
        md.microsegment_def_id,
        md.microsegment_id,
        md.microsegment_profiler_snapshot_id,

        md.microsegment_execution_date_start,
        md.microsegment_execution_date_end,

        def.microsegment_def_name,
        def.microsegment_def_description,

        /* ✅ FIX: parse string JSON */
        PARSE_JSON(md.microsegment_profile) AS microsegment_profile,

        an.anomaly_detection_method,
        an.is_within_anomaly,
        an.is_across_anomaly,
        an.within_microsegment_anomaly_reason,
        an.across_microsegment_anomaly_reason,
        an.within_microsegment_anomaly_confidence_score,
        an.across_microsegment_anomaly_confidence_score,

        an.created_timestamp AS anomaly_created_ts
    FROM microsegment_details md

    LEFT JOIN microsegment_definition def
        ON md.microsegment_def_id = def.microsegment_def_id

    LEFT JOIN microsegment_anomalies an
        ON
            md.dataset_id = an.dataset_id
            AND md.microsegment_profiler_snapshot_id = an.microsegment_profiler_snapshot_id
            AND md.microsegment_def_id = an.microsegment_def_id
            AND md.microsegment_id = an.microsegment_id
            AND md.kpi_id = an.kpi_id
),

flattened AS (
    SELECT
        b.*,
        f.key::STRING AS attribute_name,
        f.value::STRING AS attribute_value
    FROM base b,
        LATERAL FLATTEN(
            INPUT => b.microsegment_profile,
            OUTER => TRUE
        ) f
)

SELECT
    dataset_id,
    kpi_id,
    microsegment_def_id,
    microsegment_id,
    microsegment_profiler_snapshot_id,

    microsegment_def_name,
    microsegment_def_description,

    microsegment_execution_date_start AS execution_start_ts,
    microsegment_execution_date_end AS execution_end_ts,

    attribute_name,
    attribute_value,

    /* Metric detection */
    CASE
        WHEN TRY_TO_NUMBER(attribute_value) IS NOT NULL
            THEN attribute_name
        ELSE NULL
    END AS metric_name,

    TRY_TO_NUMBER(attribute_value) AS metric_value,

    /* Time grain detection */
    CASE
        WHEN TRY_TO_DATE(attribute_value) IS NOT NULL
            THEN attribute_name
        ELSE NULL
    END AS time_attribute_name,

    TRY_TO_DATE(attribute_value) AS time_attribute_value,

    /* Anomaly info */
    CASE
        WHEN is_within_anomaly THEN 'WITHIN'
        WHEN is_across_anomaly THEN 'ACROSS'
        ELSE 'NONE'
    END AS anomaly_scope,

    anomaly_detection_method,

    COALESCE(
        within_microsegment_anomaly_reason,
        across_microsegment_anomaly_reason
    ) AS anomaly_reason,

    GREATEST(
        COALESCE(within_microsegment_anomaly_confidence_score, 0),
        COALESCE(across_microsegment_anomaly_confidence_score, 0)
    ) AS confidence_score,

    anomaly_created_ts

FROM flattened;
