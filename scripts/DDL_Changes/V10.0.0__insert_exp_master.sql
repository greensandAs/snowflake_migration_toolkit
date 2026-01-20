USE DATABASE {{ snowflake_database }};
USE SCHEMA {{ snowflake_schema }};

TRUNCATE TABLE DQ_EXPECTATION_MASTER;

INSERT INTO DQ_EXPECTATION_MASTER (
    EXPECTATION_ID, VALIDATION_NAME, DESCRIPTION, DIMENSION, IS_ACTIVE, CHECK_TYPE, SUB_DIMENSION, INSTRUCTION_TEXT, DQ_ENGINE
) VALUES
(
    1,
    'expect_column_values_to_be_null',
    'Expect the column values to be null.',
    'Completeness',
    'Y',
    'COMPLETENESS',
    'Completeness',
    'Please provide the column name and 
    optionally a value for "mostly" (a float between 0 and 1) to specify the fraction of values expected to be null.',
    'SQL'
),
(
    2,
    'expect_column_values_to_not_be_null',
    'Expect the column values to not be null',
    'Completeness',
    'Y',
    'COMPLETENESS',
    'Completeness',
    'Provide column name to check non-nulls. Example: cancellation_reason',
    'SQL'
),
(
    3,
    'expect_table_row_count_to_be_between',
    'Expect the number of rows to be between two values',
    'Volume',
    'Y',
    'ROW COUNT RANGE',
    'Row Count Range',
    'Provide min and max row count. Example: 1000, 100000',
    'SQL'
),
(
    4,
    'expect_table_row_count_to_equal_other_table',
    'Expect the number of rows to equal the number in another table within the same database',
    'Volume',
    'Y',
    'ROW COUNT MATCH',
    'Row Count Match',
    'Provide table name whose row count should match. Example: customer_dim',
    'SQL'
),
(
    5,
    'expect_column_values_to_be_recent',
    'Expect date column to have values within the last 24 hours for freshness validation.',
    'Freshness',
    'Y',
    'FRESHNESS CHECK',
    'Freshness Check',
    'Provide the column name, freshness threshold amount (integer), threshold unit (e.g., ''hours'', ''minutes''), and optionally the timezone. 
    Example: last_update_date, 24, hours, UTC',
    'SQL'
),
(
    6,
    'expect_column_to_exist',
    'Checks for the existence of a specified column within a table.',
    'Schema',
    'Y',
    'COLUMN STRUCTURE',
    'Column Structure',
    'Provide the column name that must exist. Example: user_id',
    'SQL'
),
(
    7,
    'expect_column_values_to_be_of_type',
    'Expect a column to contain values of a specified data type',
    'Schema',
    'Y',
    'DATA TYPE CHECK',
    'Data Type Check',
    'Please provide the column which needs to be checked and datatype also in type_. Example: user_id,int',
    'SQL'
),
(
    8,
    'expect_table_column_count_to_be_between',
    'Expect the number of columns in a table to be between two values',
    'Schema',
    'Y',
    'COLUMN STRUCTURE',
    'Column Structure',
    'Provide minimum and maximum expected number of columns. Example: 5, 10',
    'SQL'
),
(
    9,
    'expect_table_columns_to_match_set',
    'Expect the columns in a table to match an unordered set',
    'Schema',
    'Y',
    'COLUMN STRUCTURE',
    'Column Structure',
    'Provide list of expected columns. Example: ["id", "name", "age"]',
    'SQL'
),
(
    10,
    'expect_column_values_to_be_between',
    'Expect the column entries to be between a minimum value and a maximum value (inclusive)',
    'Numeric',
    'Y',
    'RANGE CHECK',
    'Range Check',
    'Provide maximum and minimum value which needs to be checked for a given column. Example: 100,0',
    'SQL'
);
INSERT INTO DQ_EXPECTATION_MASTER (
    EXPECTATION_ID, VALIDATION_NAME, DESCRIPTION, DIMENSION, IS_ACTIVE, CHECK_TYPE, SUB_DIMENSION, INSTRUCTION_TEXT, DQ_ENGINE
) VALUES
(
    11,
    'expect_column_max_to_be_between',
    'Expect the column maximum to be between a minimum value and a maximum value',
    'Numeric',
    'Y',
    'RANGE CHECK',
    'Range Check',
    'Provide minimum and maximum value which needs to be checked for a given column. Example: 0,100',
    'SQL'
),
(
    12,
    'expect_column_mean_to_be_between',
    'Expect the column mean to be between a minimum value and a maximum value (inclusive)',
    'Numeric',
    'Y',
    'STATISTICAL CHECK',
    'Statistical Check',
    'Provide minimum and maximum values to validate against column mean. Example: 10.5, 50.0',
    'SQL'
),
(
    13,
    'expect_column_median_to_be_between',
    'Expect the column median to be between a minimum value and a maximum value.',
    'Numeric',
    'Y',
    'STATISTICAL CHECK',
    'Statistical Check',
    'Provide minimum and maximum values to validate against column median. Example: 10, 100',
    'SQL'
),
(
    14,
    'expect_column_min_to_be_between',
    'Expect the column minimum to be between a minimum value and a maximum value.',
    'Numeric',
    'Y',
    'RANGE CHECK',
    'Range Check',
    'Provide minimum and maximum value which needs to be checked for a given column. Example: 0,100',
    'SQL'
),
(
    15,
    'expect_column_pair_values_a_to_be_greater_than_b',
    'Expect the values in column A to be greater than column B',
    'Numeric',
    'Y',
    'COLUMN PAIR RANGE',
    'Column Pair Range',
    'Provide column A and column B to check A > B. Example: selling_price,cost_price',
    'SQL'
),
(
    16,
    'expect_column_stdev_to_be_between',
    'Expect the column standard deviation to be between a minimum value and a maximum value.',
    'Numeric',
    'Y',
    'STATISTICAL CHECK',
    'Statistical Check',
    'Provide minimum and maximum values for column standard deviation. Example: 5.0, 25.0',
    'SQL'
),
(
    17,
    'expect_column_sum_to_be_between',
    'Expect the column sum to be between a minimum value and a maximum value.',
    'Numeric',
    'Y',
    'RANGE CHECK',
    'Range Check',
    'Provide minimum and maximum values for column sum. Example: 100, 5000',
    'SQL'
),
(
    18,
    'expect_column_value_lengths_to_be_between',
    'Expect the column entries to be strings with length between a minimum value and a maximum value (inclusive).',
    'Validity',
    'Y',
    'LENGTH CHECK',
    'Length Check',
    'Provide min and max string lengths. Example: 5,10',
    'SQL'
),
(
    19,
    'expect_column_values_to_match_regex',
    'Expect the column entries to be strings that match a given regular expression',
    'Validity',
    'Y',
    'PATTERN MATCH',
    'Pattern Match',
    'Please provide the valid regular expression which needs to be checked. 
    Example: Valid regex: ^\\d{2}-\\d{3}-\\d{3}-\\d{4}$ Matching Input: 91-123-456-7890',
    'SQL'
),
(
    20,
    'expect_column_values_to_not_match_regex',
    'Expect the column entries to be strings that do NOT match a given regular expression',
    'Validity',
    'Y',
    'PATTERN MATCH',
    'Pattern Match',
    'Provide regex that column entries should NOT match. Example: ^ERR.*',
    'SQL'
);
INSERT INTO DQ_EXPECTATION_MASTER (
    EXPECTATION_ID, VALIDATION_NAME, DESCRIPTION, DIMENSION, IS_ACTIVE, CHECK_TYPE, SUB_DIMENSION, INSTRUCTION_TEXT, DQ_ENGINE
) VALUES
(
    21,
    'expect_column_values_to_be_in_set',
    'Expect each column value to be in a given set',
    'Validity',
    'Y',
    'DOMAIN INCLUSION',
    'Domain Inclusion',
    'Please provide the column name and the set of values expected in that column. 
    Optionally, include a value for "mostly" (a float between 0 and 1) if you want to allow a fraction of values not to be in the set.',
    'SQL'
),
(
    22,
    'expect_column_values_to_not_be_in_set',
    'Expect column entries to not be in the set',
    'Validity',
    'Y',
    'DOMAIN EXCLUSION',
    'Domain Exclusion',
    'Please provide the set of values which should not appear in the given column. Example: error,unknown',
    'SQL'
),
(
    23,
    'expect_column_unique_value_count_to_be_between',
    'Expect the number of unique values to be between a minimum value and a maximum value.',
    'Uniqueness',
    'Y',
    'UNIQUENESS DISTRIBUTION',
    'Uniqueness Distribution',
    'Provide min and max count for unique values. Example: 10, 100',
    'SQL'
),
(
    24,
    'expect_column_values_to_be_unique',
    'Expect each column value to be unique',
    'Uniqueness',
    'Y',
    'UNIQUENESS CHECK',
    'Uniqueness Check',
    'Please provide column name and optionally a value for "mostly" (float between 0 and 1) to specify the fraction of values expected to be unique.',
    'SQL'
),
(
    25,
    'unexpected_rows_expectation',
    'Expectation will fail validation if the query returns one or more rows. The WHERE clause defines the fail criteria',
    'SQL',
    'Y',
    'BUSINESS RULE',
    'Business Rule',
    'Provide the full SQL query to identify failing rows.',
    'SQL'
);

TRUNCATE TABLE DQ_EXPECTATION_ARGUMENTS;

INSERT INTO DQ_EXPECTATION_ARGUMENTS (
    EXPECTATION_ARG_ID, EXPECTATION_ID, EXPECTATION_NAME, ARGUMENT_NAME, ARGUMENT_TYPE, ARGUMENT_DESC, IS_MANDATORY, DEFAULT_VALUE
) VALUES
(101, 1, 'expect_column_values_to_be_null', 'column', 'str', 'The column name', true, NULL),
(
    102,
    1,
    'expect_column_values_to_be_null',
    'mostly',
    'number or None',
    'Successful if at least `mostly` fraction of values match the Expectation. Default 1.',
    NULL,
    NULL
),
(103, 2, 'expect_column_values_to_not_be_null', 'column', 'str', 'The column name', true, NULL),
(
    104,
    2,
    'expect_column_values_to_not_be_null',
    'mostly',
    'number or None',
    'Successful if at least `mostly` fraction of values match the Expectation. Default 1.',
    NULL,
    NULL
),
(
    105,
    3,
    'expect_table_row_count_to_be_between',
    'strict_min',
    'boolean',
    'If True, the row count must be strictly larger than min_value',
    false,
    'FALSE'
),
(
    106,
    3,
    'expect_table_row_count_to_be_between',
    'strict_max',
    'boolean',
    'If True, the row count must be strictly smaller than max_value',
    false,
    'FALSE'
),
(107, 3, 'expect_table_row_count_to_be_between', 'min_value', 'int or None', 'The minimum number of rows, inclusive', true, NULL),
(108, 3, 'expect_table_row_count_to_be_between', 'max_value', 'int or None', 'The maximum number of rows, inclusive', true, NULL),
(
    109,
    4,
    'expect_table_row_count_to_equal_other_table',
    'other_table_name',
    'str',
    'The name of the other table. Other table must be located within the same database.',
    true,
    NULL
),
(
    110,
    5,
    'expect_column_values_to_be_recent',
    'freshness_threshold_amount',
    'int',
    'The maximum time elapsed since the data was generated or updated that is still considered "fresh." This is the numerical part of the threshold.',
    true,
    NULL
);
INSERT INTO DQ_EXPECTATION_ARGUMENTS (
    EXPECTATION_ARG_ID, EXPECTATION_ID, EXPECTATION_NAME, ARGUMENT_NAME, ARGUMENT_TYPE, ARGUMENT_DESC, IS_MANDATORY, DEFAULT_VALUE
) VALUES
(
    111,
    5,
    'expect_column_values_to_be_recent',
    'freshness_threshold_unit',
    'str',
    'The unit of time for the threshold amount (e.g., ''hours'', ''seconds'', ''minutes''). This defines what the amount is measuring.',
    true,
    NULL
),
(
    112,
    5,
    'expect_column_values_to_be_recent',
    'timezone',
    'str',
    'The time zone used for the current time when calculating the age of the data. 
    This is crucial for accurate comparisons, especially when data is ingested from different geographical locations.',
    NULL,
    NULL
),
(113, 6, 'expect_column_to_exist', 'column', 'str', 'The column name', true, NULL),
(114, 7, 'expect_column_values_to_be_of_type', 'column', 'str', 'The column name', true, NULL),
(
    115,
    7,
    'expect_column_values_to_be_of_type',
    'type_',
    'str',
    'A string representing the data type that each column should have as entries. 
    Valid types are defined by the current backend implementation and are dynamically loaded',
    true,
    NULL
),
(116, 8, 'expect_table_column_count_to_be_between', 'min_value', 'int or None', 'The minimum number of columns, inclusive', true, NULL),
(117, 8, 'expect_table_column_count_to_be_between', 'max_value', 'int or None', 'The maximum number of columns, inclusive', true, NULL),
(118, 9, 'expect_table_columns_to_match_set', 'column_set', 'list of str', 'The column names, in any order', true, NULL),
(
    119,
    9,
    'expect_table_columns_to_match_set',
    'exact_match',
    'boolean',
    'If True, the list of columns must exactly match the observed columns. 
    If False, observed columns must include column_set but additional columns will pass',
    false,
    'TRUE'
),
(
    120,
    10,
    'expect_column_values_to_be_between',
    'strict_min',
    'boolean',
    'If True, values must be strictly larger than min_value. Default=False',
    false,
    'FALSE'
);
INSERT INTO DQ_EXPECTATION_ARGUMENTS (
    EXPECTATION_ARG_ID, EXPECTATION_ID, EXPECTATION_NAME, ARGUMENT_NAME, ARGUMENT_TYPE, ARGUMENT_DESC, IS_MANDATORY, DEFAULT_VALUE
) VALUES
(
    121,
    10,
    'expect_column_values_to_be_between',
    'strict_max',
    'boolean',
    'If True, values must be strictly smaller than max_value. Default=False',
    false,
    'FALSE'
),
(122, 10, 'expect_column_values_to_be_between', 'column', 'str', 'The column name', true, NULL),
(123, 10, 'expect_column_values_to_be_between', 'min_value', 'comparable type or None', 'The minimum value for a column entry', true, NULL),
(124, 10, 'expect_column_values_to_be_between', 'max_value', 'comparable type or None', 'The maximum value for a column entry', true, NULL),
(
    125,
    10,
    'expect_column_values_to_be_between',
    'mostly',
    'number or None',
    'Successful if at least `mostly` fraction of values match the Expectation. Default 1.',
    NULL,
    NULL
),
(
    126,
    11,
    'expect_column_max_to_be_between',
    'strict_min ',
    'boolean',
    'If True, the minimal column minimum must be strictly larger than min_value, default=False',
    false,
    'FALSE'
),
(
    127,
    11,
    'expect_column_max_to_be_between',
    'strict_max ',
    'boolean',
    'If True, the maximal column minimum must be strictly smaller than max_value, default=False',
    false,
    'FALSE'
),
(128, 11, 'expect_column_max_to_be_between', 'column', 'str', 'The column name', true, NULL),
(129, 11, 'expect_column_max_to_be_between', 'min_value', 'comparable type or None', 'The minimal column minimum allowed', true, NULL),
(130, 11, 'expect_column_max_to_be_between', 'max_value', 'comparable type or None', 'The maximal column minimum allowed.', true, NULL);
INSERT INTO DQ_EXPECTATION_ARGUMENTS (
    EXPECTATION_ARG_ID, EXPECTATION_ID, EXPECTATION_NAME, ARGUMENT_NAME, ARGUMENT_TYPE, ARGUMENT_DESC, IS_MANDATORY, DEFAULT_VALUE
) VALUES
(
    131,
    12,
    'expect_column_mean_to_be_between',
    'strict_min ',
    'boolean',
    'If True, the column mean must be strictly larger than min_value, default=False',
    false,
    'FALSE'
),
(
    132,
    12,
    'expect_column_mean_to_be_between',
    'strict_max ',
    'boolean',
    'If True, the column mean must be strictly smaller than max_value, default=False',
    false,
    'FALSE'
),
(133, 12, 'expect_column_mean_to_be_between', 'column', 'str', 'The column name', true, NULL),
(134, 12, 'expect_column_mean_to_be_between', 'min_value ', 'float or None', 'The minimum value for the column mean.', true, NULL),
(135, 12, 'expect_column_mean_to_be_between', 'max_value ', 'float or None', 'The maximum value for the column mean', true, NULL),
(
    136,
    13,
    'expect_column_median_to_be_between',
    'strict_min ',
    'boolean',
    'If True, the column median must be strictly larger than min_value, default=False',
    false,
    'FALSE'
),
(
    137,
    13,
    'expect_column_median_to_be_between',
    'strict_max ',
    'boolean',
    'If True, the column median must be strictly smaller than max_value, default=False',
    false,
    'FALSE'
),
(138, 13, 'expect_column_median_to_be_between', 'column', 'str', 'The column name', true, NULL),
(139, 13, 'expect_column_median_to_be_between', 'min_value ', 'int or None', 'The minimum value for the column median', true, NULL),
(140, 13, 'expect_column_median_to_be_between', 'max_value ', 'int or None', 'The maximum value for the column median', true, NULL);
INSERT INTO DQ_EXPECTATION_ARGUMENTS (
    EXPECTATION_ARG_ID, EXPECTATION_ID, EXPECTATION_NAME, ARGUMENT_NAME, ARGUMENT_TYPE, ARGUMENT_DESC, IS_MANDATORY, DEFAULT_VALUE
) VALUES
(
    141,
    14,
    'expect_column_min_to_be_between',
    'strict_min ',
    'boolean',
    'If True, the minimal column minimum must be strictly larger than min_value, default=False',
    false,
    'FALSE'
),
(
    142,
    14,
    'expect_column_min_to_be_between',
    'strict_max ',
    'boolean',
    'If True, the maximal column minimum must be strictly smaller than max_value, default=False',
    false,
    'FALSE'
),
(143, 14, 'expect_column_min_to_be_between', 'column ', 'str', 'The column name', true, NULL),
(144, 14, 'expect_column_min_to_be_between', 'min_value', 'comparable type or None', 'The minimal column minimum allowed', true, NULL),
(145, 14, 'expect_column_min_to_be_between', 'max_value', 'comparable type or None', 'The maximal column minimum allowed.', true, NULL),
(
    146,
    15,
    'expect_column_pair_values_a_to_be_greater_than_b',
    'ignore_row_if',
    'str',
    ' "both_values_are_missing", "either_value_is_missing", "neither" If specified, sets the condition on which a given row is to be ignored
    . Default "neither".',
    NULL,
    NULL
),
(
    147,
    15,
    'expect_column_pair_values_a_to_be_greater_than_b',
    'mostly',
    'number or None',
    'Successful if at least `mostly` fraction of values match the Expectation. Default 1.',
    NULL,
    NULL
),
(
    148,
    15,
    'expect_column_pair_values_a_to_be_greater_than_b',
    'or_equal',
    'boolean or None',
    'If True, then values can be equal, not strictly greater',
    false,
    NULL
),
(149, 15, 'expect_column_pair_values_a_to_be_greater_than_b', 'column_A', 'str', 'The first column name', true, NULL),
(150, 15, 'expect_column_pair_values_a_to_be_greater_than_b', 'column_B', 'str', 'The second column name', true, NULL);
INSERT INTO DQ_EXPECTATION_ARGUMENTS (
    EXPECTATION_ARG_ID, EXPECTATION_ID, EXPECTATION_NAME, ARGUMENT_NAME, ARGUMENT_TYPE, ARGUMENT_DESC, IS_MANDATORY, DEFAULT_VALUE
) VALUES
(
    151,
    16,
    'expect_column_stdev_to_be_between',
    'strict_min',
    'boolean',
    'If True, the column standard deviation must be strictly larger than min_value',
    false,
    'FALSE'
),
(
    152,
    16,
    'expect_column_stdev_to_be_between',
    'strict_max',
    'boolean',
    'If True, the column standard deviation must be strictly smaller than max_value',
    false,
    'FALSE'
),
(153, 16, 'expect_column_stdev_to_be_between', 'column', 'str', 'The column name', true, NULL),
(154, 16, 'expect_column_stdev_to_be_between', 'min_value', 'float or None', 'The minimum value for the column standard deviation', true, NULL),
(155, 16, 'expect_column_stdev_to_be_between', 'max_value', 'float or None', 'The maximum value for the column standard deviation', true, NULL),
(
    156,
    17,
    'expect_column_sum_to_be_between',
    'strict_min',
    'boolean',
    'If True, the minimal sum must be strictly larger than min_value',
    false,
    'FALSE'
),
(
    157,
    17,
    'expect_column_sum_to_be_between',
    'strict_max',
    'boolean',
    'If True, the maximal sum must be strictly smaller than max_value',
    false,
    'FALSE'
),
(158, 17, 'expect_column_sum_to_be_between', 'column', 'str', 'The column name', true, NULL),
(159, 17, 'expect_column_sum_to_be_between', 'min_value', 'comparable type or None', 'The minimal sum allowed', true, NULL),
(160, 17, 'expect_column_sum_to_be_between', 'max_value', 'comparable type or None', 'The maximal sum allowed', true, NULL);
INSERT INTO DQ_EXPECTATION_ARGUMENTS (
    EXPECTATION_ARG_ID, EXPECTATION_ID, EXPECTATION_NAME, ARGUMENT_NAME, ARGUMENT_TYPE, ARGUMENT_DESC, IS_MANDATORY, DEFAULT_VALUE
) VALUES
(
    161,
    18,
    'expect_column_value_lengths_to_be_between',
    'strict_min',
    'bool',
    'If True, values must be strictly larger than min_value',
    false,
    'FALSE'
),
(
    162,
    18,
    'expect_column_value_lengths_to_be_between',
    'strict_max',
    'bool',
    'If True, values must be strictly smaller than max_value',
    false,
    'FALSE'
),
(163, 18, 'expect_column_value_lengths_to_be_between', 'column', 'str', 'The column name', true, NULL),
(164, 18, 'expect_column_value_lengths_to_be_between', 'min_value', 'int or None', 'The minimum value for a column entry length', true, NULL),
(
    165,
    18,
    'expect_column_value_lengths_to_be_between',
    'mostly',
    'number or None',
    'Successful if at least `mostly` fraction of values match the Expectation. Default 1.',
    NULL,
    NULL
),
(166, 18, 'expect_column_value_lengths_to_be_between', 'max_value', 'int or None', 'The maximum value for a column entry length', true, NULL),
(167, 19, 'expect_column_values_to_match_regex', 'column', 'str', 'The column name', true, NULL),
(
    168,
    19,
    'expect_column_values_to_match_regex',
    'mostly',
    'number or None',
    'Successful if at least `mostly` fraction of values match the Expectation. Default 1.',
    NULL,
    NULL
),
(169, 19, 'expect_column_values_to_match_regex', 'regex', 'str', 'The regular expression the column entries should match', true, NULL),
(170, 20, 'expect_column_values_to_not_match_regex', 'column', 'str', 'The column name', true, NULL);
INSERT INTO DQ_EXPECTATION_ARGUMENTS (
    EXPECTATION_ARG_ID, EXPECTATION_ID, EXPECTATION_NAME, ARGUMENT_NAME, ARGUMENT_TYPE, ARGUMENT_DESC, IS_MANDATORY, DEFAULT_VALUE
) VALUES
(
    171,
    20,
    'expect_column_values_to_not_match_regex',
    'mostly',
    'number or None',
    'Successful if at least `mostly` fraction of values match the Expectation. Default 1.',
    NULL,
    NULL
),
(172, 20, 'expect_column_values_to_not_match_regex', 'regex', 'str', 'The regular expression the column entries should NOT match.', true, NULL),
(173, 21, 'expect_column_values_to_be_in_set', 'column', 'str', 'The column name', true, NULL),
(
    174,
    21,
    'expect_column_values_to_be_in_set',
    'mostly',
    'number or None',
    'Successful if at least `mostly` fraction of values match the Expectation. Default 1.',
    NULL,
    NULL
),
(175, 21, 'expect_column_values_to_be_in_set', 'value_set', 'set-like', 'A set of objects used for comparison.', true, NULL),
(176, 22, 'expect_column_values_to_not_be_in_set', 'column', 'str', 'The column name', true, NULL),
(
    177,
    22,
    'expect_column_values_to_not_be_in_set',
    'mostly',
    'number or None',
    'Successful if at least `mostly` fraction of values match the Expectation. Default 1.',
    NULL,
    NULL
),
(178, 22, 'expect_column_values_to_not_be_in_set', 'value_set', 'set-like', 'A set of objects used for comparison.', true, NULL),
(
    179,
    23,
    'expect_column_unique_value_count_to_be_between',
    'strict_min',
    'bool',
    'If True, the column must have strictly more unique value count than min_value to pass',
    false,
    'FALSE'
),
(
    180,
    23,
    'expect_column_unique_value_count_to_be_between',
    'strict_max',
    'bool',
    'If True, the column must have strictly fewer unique value count than max_value to pass',
    false,
    'FALSE'
);
INSERT INTO DQ_EXPECTATION_ARGUMENTS (
    EXPECTATION_ARG_ID, EXPECTATION_ID, EXPECTATION_NAME, ARGUMENT_NAME, ARGUMENT_TYPE, ARGUMENT_DESC, IS_MANDATORY, DEFAULT_VALUE
) VALUES
(181, 23, 'expect_column_unique_value_count_to_be_between', 'column', 'str', 'The column name', true, NULL),
(182, 23, 'expect_column_unique_value_count_to_be_between', 'min_value', 'int or None', 'The minimum number of unique values allowed', true, NULL),
(183, 23, 'expect_column_unique_value_count_to_be_between', 'max_value', 'int or None', 'The maximum number of unique values allowed', true, NULL),
(
    184,
    24,
    'expect_column_values_to_be_unique',
    'mostly',
    'number or None',
    'Successful if at least `mostly` fraction of values match the Expectation. Default 1.',
    NULL,
    NULL
),
(185, 24, 'expect_column_values_to_be_unique', 'column', 'str', 'The column name', true, NULL),
(
    186,
    25,
    'unexpected_rows_expectation',
    'unexpected_rows_query',
    'str',
    'A SQL to be executed for validation.Provide fully qualified table (dbname.schemaname.table)',
    true,
    NULL
);
