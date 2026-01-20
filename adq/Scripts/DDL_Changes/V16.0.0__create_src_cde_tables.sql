USE DATABASE {{ snowflake_database }};
USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE TABLE SRC_CDE_RULES (
    RULE_ID NUMBER(38, 0) AUTOINCREMENT START 1 INCREMENT 1 NOT NULL,
    RULE_NAME VARCHAR(255) NOT NULL,
    RULE_DESCRIPTION STRING,
    CREATED_BY VARCHAR(100),
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_BY VARCHAR(100),
    UPDATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE SRC_CDE_RULES_WEIGHTAGE (
    DATASET_ID NUMBER(38, 0) NOT NULL,
    RULE_ID NUMBER(38, 0) NOT NULL,
    WEIGHTAGE NUMBER(5, 2) NOT NULL,
    THRESHOLD NUMBER(5, 2) NOT NULL,
    CREATED_BY VARCHAR(100),
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_BY VARCHAR(100),
    UPDATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    IS_SELECTED BOOLEAN DEFAULT FALSE
);


INSERT INTO SRC_CDE_RULES (
    RULE_NAME,
    RULE_DESCRIPTION,
    CREATED_BY,
    CREATED_TIMESTAMP,
    UPDATED_BY,
    UPDATED_TIMESTAMP
)
VALUES
(
    'Regulatory/Compliance Impact',
    $$Is the data required for submission to regulatory bodies
(FDA, EMA) or subject to GxP regulations?
Data is included in a Batch Production Record (BPR) or
Certificate of Analysis (CoA).
High (9-10)$$,
    'admin',
    CURRENT_TIMESTAMP(),
    NULL,
    CURRENT_TIMESTAMP()
),
(
    'Patient Safety/Product Quality Impact',
    $$Does the inaccuracy or absence of this data directly affect final product
quality or patient safety?
Data represents a Critical Process Parameter (CPP) like
reactor temperature or pH levels.
High (8-10)$$,
    'admin',
    CURRENT_TIMESTAMP(),
    NULL,
    CURRENT_TIMESTAMP()
),
(
    'Business/Operational Impact',
    $$Would this data’s unavailability cause significant operational downtime,
financial loss, or supply chain disruption?
Data is essential for production scheduling or inventory management
of high-value raw materials.
Medium (5-7)$$,
    'admin',
    CURRENT_TIMESTAMP(),
    NULL,
    CURRENT_TIMESTAMP()
),
(
    'Key Decision Making/KPI',
    $$Is this data used to calculate key performance indicators (KPIs)
or inform critical management decisions?
Data is used in a quality assurance dashboard to determine
batch release readiness.
Medium (4-6)$$,
    'admin',
    CURRENT_TIMESTAMP(),
    NULL,
    CURRENT_TIMESTAMP()
),
(
    'Data Sensitivity/Confidentiality',
    $$Does the data contain sensitive intellectual property
(e.g., proprietary formulas) or PII
(Personally Identifiable Information)?
Data includes specific drug compound formulas or
employee medical leave information.
Medium (4-6)$$,
    '1006',
    CURRENT_TIMESTAMP(),
    '1006',
    CURRENT_TIMESTAMP()
),
(
    'System Dependency/Usage',
    $$Is the data element used across multiple systems or
does it trigger many downstream processes?
The "Batch ID" field is used in MES, LIMS, ERP, and QMS systems.
Low-Medium (3-5)$$,
    '1006',
    CURRENT_TIMESTAMP(),
    '1006',
    CURRENT_TIMESTAMP()
);
