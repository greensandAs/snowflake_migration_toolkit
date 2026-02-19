USE DATABASE {{ snowflake_database }};

USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE VIEW V_MICROSEGMENT_ANOMALY_FLAT_V1 (
    DATASET_ID,
    KPI_ID,
    MICROSEGMENT_DEF_ID,
    MICROSEGMENT_DEF_NAME,
    MICROSEGMENT_PROFILER_SNAPSHOT_ID,
    MICROSEGMENT_ID,
    ATTRIBUTE_NAME,
    ATTRIBUTE_VALUE,
    METRIC_COLUMN,
    TIME_COLUMN,
    CATEGORICAL_COLUMN,
    IS_ANOMALY,
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

        def.microsegment_def_name,

        PARSE_JSON(md.microsegment_profile) AS microsegment_profile,

        /* anomaly flags */
        CASE
            WHEN
                an.is_within_anomaly = TRUE
                OR an.is_across_anomaly = TRUE
                THEN TRUE
            ELSE FALSE
        END AS is_anomaly,

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
    microsegment_def_name,
    microsegment_profiler_snapshot_id,
    microsegment_id,

    attribute_name,
    attribute_value,

    /* metric column */
    CASE
        WHEN TRY_TO_NUMBER(attribute_value) IS NOT NULL
            THEN attribute_name
        ELSE NULL
    END AS metric_column,

    /* time column - dates that are not numeric values */
    CASE
        WHEN
            TRY_TO_DATE(attribute_value) IS NOT NULL
            AND TRY_TO_NUMBER(attribute_value) IS NULL
            THEN attribute_name
        ELSE NULL
    END AS time_column,

    /* categorical column */
    CASE
        WHEN
            TRY_TO_NUMBER(attribute_value) IS NULL
            AND TRY_TO_DATE(attribute_value) IS NULL
            THEN attribute_name
        ELSE NULL
    END AS categorical_column,

    is_anomaly,

    /* anomaly scope */
    CASE
        WHEN is_within_anomaly THEN 'WITHIN'
        WHEN is_across_anomaly THEN 'ACROSS'
        ELSE NULL
    END AS anomaly_scope,

    /* anomaly metadata only meaningful when is_anomaly = true */
    anomaly_detection_method,

    CASE
        WHEN is_anomaly
            THEN COALESCE(
                within_microsegment_anomaly_reason,
                across_microsegment_anomaly_reason
            )
        ELSE NULL
    END AS anomaly_reason,

    CASE
        WHEN is_anomaly
            THEN GREATEST(
                COALESCE(within_microsegment_anomaly_confidence_score, 0),
                COALESCE(across_microsegment_anomaly_confidence_score, 0)
            )
        ELSE NULL
    END AS confidence_score,

    anomaly_created_ts

FROM flattened;
