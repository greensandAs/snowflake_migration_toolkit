USE DATABASE {{ snowflake_database }};
USE SCHEMA {{ snowflake_schema }};

ALTER TABLE DQ_RULE_AUDIT_LOG
ADD COLUMN FAILED_ROWS VARIANT;

ALTER TABLE DQ_RULE_AUDIT_LOG
ADD COLUMN SESSION_ID VARCHAR;


INSERT INTO DQ_JOB_EXEC_CONFIG (
    DQ_DB_NAME, DQ_SCHEMA_NAME, SAMPLE_ROWS_CNT, SUCCESS_CODE, FAILED_CODE, EXECUTION_ERROR, CREATED_AT, UPDATED_AT
) VALUES
('LTP_DQ_AUGMENTED', 'CURATED', 'ALL', '100', '200', '400', CURRENT_TIMESTAMP(), NULL);


INSERT INTO DQ_EXPECTATION_MASTER (
    EXPECTATION_ID,
    VALIDATION_NAME,
    DESCRIPTION,
    DIMENSION,
    IS_ACTIVE,
    CHECK_TYPE,
    SUB_DIMENSION,
    INSTRUCTION_TEXT,
    DQ_ENGINE
)
VALUES
(
    1,
    'expect_table_row_count_to_be_between',
    'Expect the number of rows to be between two values',
    'Volume',
    NULL,
    'GE_ROW_COUNT',
    'Row Count Range',
    'Provide min and max row count. Example: 1000, 100000',
    'SQL'
),
(
    2,
    'expect_column_values_to_match_like_pattern_list',
    'Expect the column entries to be strings that match any of a provided
    list of like pattern expressions',
    'Validity',
    NULL,
    'GE_PATTERN_MATCH',
    'Pattern Match',
    'Provide list of LIKE patterns to match. Example: ["%ERR%", "%FAIL%"]',
    'SQL'
),
(
    3,
    'expect_column_values_to_be_unique',
    'Expect each column value to be unique',
    'Uniqueness',
    'Y',
    'GE_UNIQUENESS',
    'Uniqueness Check',
    'Provide the column to check for uniqueness. Example: transaction_id',
    'SQL'
),
(
    4,
    'expect_column_to_exist',
    'Checks for the existence of a specified column within a table.',
    'Schema',
    'Y',
    'GE_COLUMN_STRUCTURE',
    'Column Structure',
    'Provide the column name that must exist. Example: user_id',
    'SQL'
),
(
    5,
    'expect_column_values_to_be_between',
    'Expect the column entries to be between a minimum
    value and a maximum value (inclusive)',
    'Numeric',
    'Y',
    'GE_NUMERIC_BOUNDS',
    'Range Check',
    'Provide maximum and minimum value which needs to be
    checked for a given column. Example: 100,0',
    'SQL'
),
(
    6,
    'expect_column_median_to_be_between',
    'Expect the column median to be between a minimum value
    and a maximum value.',
    'Numeric',
    NULL,
    'GE_NUMERIC_BOUNDS',
    'Statistical Check',
    'Provide minimum and maximum values to validate against
    column median. Example: 10, 100',
    'SQL'
),
(
    7,
    'expect_column_mean_to_be_between',
    'Expect the column mean to be between a minimum value
    and a maximum value (inclusive)',
    'Numeric',
    NULL,
    'GE_NUMERIC_BOUNDS',
    'Statistical Check',
    'Provide minimum and maximum values to validate
    against column mean. Example: 10.5, 50.0',
    'SQL'
),
(
    8,
    'expect_column_values_to_be_null',
    'Expect the column values to be null.',
    'Completeness',
    'Y',
    'GE_NULLNESS',
    'Completeness',
    'Please provide the column which needs to be checked
    and value for mostly if applicable. Example: cancellation_reason,0.25',
    'SQL'
),
(
    9,
    'expect_column_values_to_not_be_null',
    'Expect the column values to not be null',
    'Completeness',
    'Y',
    'GE_NULLNESS',
    'Completeness',
    'Provide column name to check non-nulls. Example: cancellation_reason',
    'SQL'
),
(
    11,
    'expect_column_values_to_be_of_type',
    'Expect a column to contain values of a specified data type',
    'Schema',
    'Y',
    'GE_DATA_TYPE',
    'Data Type Check',
    'Please provide the column which needs to be checked
    and datatype also in type_. Example: user_id,int',
    'SQL'
);

INSERT INTO DQ_EXPECTATION_HANDLER_MAPPING (
    CHECK_TYPE,
    EXPECTATION_TYPE,
    SP_NAME,
    HANDLER_VERSION,
    IS_ACTIVE,
    CREATED_AT,
    UPDATED_AT
)
VALUES
('VALUE_IN_SET_CHECK', 'expect_column_values_to_be_in_set', 'SP_VALUE_IN_SET_CHECK', 'v1.0', true, CURRENT_TIMESTAMP(), NULL),
('RANGE_VALUE_CHECK', 'expect_column_values_to_be_between', 'SP_RANGE_VALUE_CHECK', 'v1.0', true, CURRENT_TIMESTAMP(), NULL),
('MEAN_RANGE_CHECK', 'expect_column_mean_to_be_between', 'SP_MEAN_RANGE_CHECK', 'v1.0', true, CURRENT_TIMESTAMP(), NULL),
('MEDIAN_RANGE_CHECK', 'expect_column_median_to_be_between', 'SP_MEDIAN_RANGE_CHECK', 'v1.0', true, CURRENT_TIMESTAMP(), NULL),
('TABLE_ROW_COUNT_CHECK', 'expect_table_row_count_to_be_between', 'SP_TABLE_ROW_COUNT_CHECK', 'v1.0', true, CURRENT_TIMESTAMP(), NULL),
('COLUMN_EXISTS_CHECK', 'expect_column_to_exist', 'SP_COLUMN_TO_EXIST_CHECK', 'v1.0', true, CURRENT_TIMESTAMP(), NULL),
('DISTINCT_IN_SET_CHECK', 'expect_column_distinct_values_to_be_in_set', 'SP_DISTINCT_VALUES_IN_SET_CHECK', 'v1.0', true, CURRENT_TIMESTAMP(), NULL),
('DATA_TYPE_CHECK', 'expect_column_values_to_be_of_type', 'SP_DATA_TYPE_CHECK', 'v1.0', true, CURRENT_TIMESTAMP(), NULL),
('DATE_FORMAT_CHECK', 'expect_column_values_to_be_in_given_format', 'SP_DATE_FORMAT_CHECK', 'v1.0', true, CURRENT_TIMESTAMP(), NULL),
('FRESHNESS_CHECK', 'expect_column_values_to_be_recent', 'SP_FRESHNESS_CHECK', 'v1.0', true, CURRENT_TIMESTAMP(), NULL),
('NOT_NULL_CHECK', 'expect_column_values_to_not_be_null', 'SP_NOT_NULL_CHECK', 'v1.0', true, CURRENT_TIMESTAMP(), NULL),
('REGEX_MATCH_CHECK', 'expect_column_values_to_match_regex', 'SP_REGEX_MATCH_CHECK', 'v1.0', true, CURRENT_TIMESTAMP(), NULL),
('REGEX_NOT_MATCH_CHECK', 'expect_column_values_to_not_match_regex', 'SP_REGEX_NOT_MATCH_CHECK', 'v1.0', true, CURRENT_TIMESTAMP(), NULL),
('NULL_ONLY_CHECK', 'expect_column_values_to_be_null', 'SP_NULL_ONLY_CHECK', 'v1.0', true, CURRENT_TIMESTAMP(), NULL),
('UNIQUENESS_CHECK', 'expect_column_values_to_be_unique', 'SP_UNIQUENESS_CHECK', 'v1.0', true, CURRENT_TIMESTAMP(), NULL),
(
    'PAIR_VALUES_A_GREATER_THAN_B',
    'expect_column_pair_values_a_to_be_greater_than_b',
    'SP_COLUMN_PAIR_A_GREATER_THAN_B',
    'v1.0',
    true,
    CURRENT_TIMESTAMP(),
    NULL
),
(
    'COMPOUND_COLUMNS_TO_BE_UNIQUE',
    'expect_compound_columns_to_be_unique',
    'SP_COMPOUND_COLUMNS_UNIQUE_CHECK',
    'v1.0',
    true,
    CURRENT_TIMESTAMP(),
    NULL
),
(
    'COLUMN_VALUE_Z_SCORES_TO_BE_LESS_THAN',
    'expect_column_value_z_scores_to_be_less_than',
    'SP_ZSCORE_LESS_THAN_CHECK',
    'v1.0',
    true,
    CURRENT_TIMESTAMP(),
    NULL
),
('SUM_RANGE_CHECK', 'expect_column_sum_to_be_between', 'SP_SUM_RANGE_CHECK', 'v1.0', true, CURRENT_TIMESTAMP(), NULL),
('MULTI_COL_SUM_CHECK', 'expect_multicolumn_sum_to_equal', 'SP_MULTICOLUMN_SUM_EQUAL_CHECK', 'v1.0', true, CURRENT_TIMESTAMP(), NULL),
('MIN_RANGE_CHECK', 'expect_column_min_to_be_between', 'SP_MIN_VALUE_BETWEEN_CHECK', 'v1.0', true, CURRENT_TIMESTAMP(), NULL),
('MAX_RANGE_CHECK', 'expect_column_max_to_be_between', 'SP_MAX_VALUE_BETWEEN_CHECK', 'v1.0', true, CURRENT_TIMESTAMP(), NULL),
('STDEV_RANGE_CHECK', 'expect_column_stdev_to_be_between', 'SP_STDEV_VALUE_BETWEEN_CHECK', 'v1.0', true, CURRENT_TIMESTAMP(), NULL),
('COMMON_VALUE_IN_SET', 'expect_column_most_common_value_to_be_in_set', 'SP_MOST_COMMON_VALUE_IN_SET_CHECK', 'v1.0', true, CURRENT_TIMESTAMP(), NULL),
('VALUE_LENGTH_RANGE_CHECK', 'expect_column_value_lengths_to_be_between', 'SP_VALUE_LENGTH_BETWEEN_CHECK', 'v1.0', true, CURRENT_TIMESTAMP(), NULL),
(
    'PROPORTIONAL_NULL_CHECK',
    'expect_column_proportion_of_non_null_value_to_be_between',
    'SP_PROPORTION_NON_NULL_BETWEEN_CHECK',
    'v1.0',
    true,
    CURRENT_TIMESTAMP(),
    NULL
),
(
    'TABLE_ROW_COUNT_CMP_CHECK',
    'expect_table_row_count_to_equal_other_table',
    'SP_TABLE_ROW_COUNT_EQUAL_OTHER_TABLE_CHECK',
    'v1.0',
    true,
    CURRENT_TIMESTAMP(),
    NULL
),
(
    'COLUMN_COUNT_RANGE_CHECK',
    'expect_table_column_count_to_be_between',
    'SP_TABLE_COLUMN_COUNT_BETWEEN_CHECK',
    'v1.0',
    true,
    CURRENT_TIMESTAMP(),
    NULL
),
('CUSTOM_SQL_CHECK', 'unexpected_rows_expectation', 'SP_UNEXPECTED_ROWS_CHECK', 'v1.0', true, CURRENT_TIMESTAMP(), NULL),
('VALUE_LENGTH_EQUAL_CHECK', 'expect_column_value_lengths_to_equal', 'SP_VALUE_LENGTH_EQUAL_CHECK', 'v1.0', true, CURRENT_TIMESTAMP(), NULL),
('TABLE_ROW_COUNT_EQUAL_CHECK', 'expect_table_row_count_to_equal', 'SP_TABLE_ROW_COUNT_EQUAL_CHECK', 'v1.0', true, CURRENT_TIMESTAMP(), NULL),
('COLUMN_COUNT_EQUAL_CHECK', 'expect_table_column_count_to_equal', 'SP_TABLE_COLUMN_COUNT_EQUAL_CHECK', 'v1.0', true, CURRENT_TIMESTAMP(), NULL),
('VALUE_NOT_IN_CHECK', 'expect_column_values_to_not_be_in_set', 'SP_VALUE_NOT_IN_SET_CHECK', 'v1.0', true, CURRENT_TIMESTAMP(), NULL),
('COLUMN_PAIR_EQUAL_CHECK', 'expect_column_pair_values_to_be_equal', 'SP_COLUMN_PAIR_EQUAL_CHECK', 'v1.0', true, CURRENT_TIMESTAMP(), NULL),
('REGEX_MATCH_LIST_CHECK', 'expect_column_values_to_match_regex_list', 'SP_REGEX_MATCH_LIST_CHECK', 'v1.0', true, CURRENT_TIMESTAMP(), NULL),
(
    'REGEX_NOT_MATCH_LIST_CHECK',
    'expect_column_values_to_not_match_regex_list',
    'SP_REGEX_NOT_MATCH_LIST_CHECK',
    'v1.0',
    true,
    CURRENT_TIMESTAMP(),
    NULL
),
('COLUMN_PAIR_IN_SET_CHECK', 'expect_column_pair_values_to_be_in_set', 'SP_COLUMN_PAIR_IN_SET_CHECK', 'v1.0', true, CURRENT_TIMESTAMP(), NULL),
(
    'PROPORTION_UNIQUE_VALUE_BETWEEN_CHECK',
    'expect_column_proportion_of_unique_values_to_be_between',
    'SP_PROPORTION_UNIQUE_VALUE_BETWEEN_CHECK',
    'v1.0',
    true,
    CURRENT_TIMESTAMP(),
    NULL
),
(
    'UNIQUE_VALUE_COUNT_BETWEEN_CHECK',
    'expect_column_unique_value_count_to_be_between',
    'SP_UNIQUE_VALUE_COUNT_BETWEEN_CHECK',
    'v1.0',
    true,
    CURRENT_TIMESTAMP(),
    NULL
),
(
    'UNIQUE_VALUES_WITHIN_RECORD_CHECK',
    'expect_select_column_values_to_be_unique_within_record',
    'SP_UNIQUE_VALUES_WITHIN_RECORD_CHECK',
    'v1.0',
    true,
    CURRENT_TIMESTAMP(),
    NULL
),
(
    'COLUMN_DISTINCT_VALUES_TO_CONTAIN_SET',
    'expect_column_distinct_values_to_contain_set',
    'SP_COLUMN_DISTINCT_VALUES_TO_CONTAIN_SET',
    'v1.0',
    true,
    CURRENT_TIMESTAMP(),
    NULL
),
('TABLE_COLUMNS_MATCH_SET_CHECK', 'expect_table_columns_to_match_set', 'SP_TABLE_COLUMNS_MATCH_SET_CHECK', 'v1.0', true, CURRENT_TIMESTAMP(), NULL),
(
    'TABLE_COLUMNS_MATCH_ORDERED_LIST_CHECK',
    'expect_table_columns_to_match_ordered_list',
    'SP_TABLE_COLUMNS_MATCH_ORDERED_LIST_CHECK',
    'v1.0',
    true,
    CURRENT_TIMESTAMP(),
    NULL
),
(
    'DISTINCT_VALUES_EQUAL_SET_CHECK',
    'expect_column_distinct_values_to_equal_set',
    'SP_DISTINCT_VALUES_EQUAL_SET_CHECK',
    'v1.0',
    true,
    CURRENT_TIMESTAMP(),
    NULL
);
