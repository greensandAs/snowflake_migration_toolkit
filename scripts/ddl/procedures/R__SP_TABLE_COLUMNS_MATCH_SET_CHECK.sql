USE DATABASE {{ snowflake_database }};

USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE PROCEDURE "SP_TABLE_COLUMNS_MATCH_SET_CHECK"("RULE" VARIANT)
RETURNS NUMBER(38, 0)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    -- Standard Framework Variables
    v_sql TEXT;
    v_result RESULTSET;
    v_total INT DEFAULT 0; -- Total expected columns
    v_unexpected INT DEFAULT 0; -- Number of columns that did not match (total mismatches)
    v_percent FLOAT DEFAULT 0;
    v_status_code NUMBER;
    v_allowed_deviation FLOAT DEFAULT 0;
    v_error_message STRING;
    v_step STRING DEFAULT ''INITIALIZATION'';
    v_run_id NUMBER DEFAULT -1;
    v_column_nm STRING;
    v_database_name STRING;
    v_schema_name STRING;
    v_table_name STRING;
    v_data_asset_id NUMBER;
    v_check_config_id NUMBER;
    v_expectation_id NUMBER;
    v_run_name STRING;
    v_data_asset_name STRING;
    v_expectation_name STRING;
    v_dq_db_name STRING;
    v_dq_schema_name STRING;
    v_success_code NUMBER;
    v_failed_code NUMBER;
    v_execution_error NUMBER;
    v_failed_records_table STRING;
    v_failed_rows_cnt_limit NUMBER;
    v_kwargs_variant VARIANT;
    v_batch_id NUMBER DEFAULT -1;
    v_procedure_name STRING;
    v_input_rule_str STRING;
    v_log_message STRING;
    v_rows_inserted NUMBER DEFAULT 0;
    v_failed_rows_threshold INT DEFAULT 10000000;
    
    -- Check specific variables
    v_exact_match BOOLEAN;
    v_dataset_type STRING;
    v_sql_query STRING;
    
    -- Metadata/Result variables
    v_actual_columns ARRAY;
    v_expected_columns ARRAY; -- Final processed array of expected columns
    v_unexpected_columns ARRAY DEFAULT ARRAY_CONSTRUCT();
    v_missing_columns ARRAY DEFAULT ARRAY_CONSTRUCT();
    v_comparison_set_expected ARRAY;
    v_comparison_set_actual ARRAY;
    v_actual_total INT DEFAULT 0;
    v_set_mismatches INT DEFAULT 0;
    v_success_flag BOOLEAN DEFAULT FALSE;
    
    -- Variables for DQ_RULE_RESULTS table compatibility (mostly NULL for this check)
    v_observed_value VARIANT;
    v_missing_count INT DEFAULT 0;
    v_missing_percent FLOAT DEFAULT 0;
    v_unexpected_percent_nonmissing FLOAT DEFAULT 0;
    v_unexpected_percent_total FLOAT DEFAULT 0;
    v_partial_unexpected_list VARIANT;
    v_unexpected_rows VARIANT;
    
    -- Variables to fetch results from the single comparison query (Used in Step 3 SELECT INTO)
    v_calculated_unexpected_cols ARRAY;
    v_calculated_missing_cols ARRAY;
    v_calculated_mismatches INT;
    v_calculated_success_flag BOOLEAN;
    v_calculated_percent FLOAT;

BEGIN
    v_input_rule_str := TO_VARCHAR(RULE);

    ----------------------------------------------------------------------------------------------------
    -- 1. Load Configuration
    v_step := ''CONFIG_LOADING'';
    v_procedure_name := COALESCE(RULE:PROCEDURE_NAME::STRING, ''SP_TABLE_COLUMNS_MATCH_SET_CHECK'');
    v_run_id := COALESCE(RULE:DATASET_RUN_ID::NUMBER, -1);
    v_check_config_id := COALESCE(RULE:RULE_CONFIG_ID::NUMBER, -1);

    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Loading configuration'');
    BEGIN
        v_sql := ''SELECT DQ_DB_NAME, DQ_SCHEMA_NAME, SUCCESS_CODE, FAILED_CODE, EXECUTION_ERROR FROM DQ_JOB_EXEC_CONFIG WHERE DQ_DB_NAME = ''''DQ_FRAMEWORK'''' AND DQ_SCHEMA_NAME = ''''METADATA'''''';
        v_result := (EXECUTE IMMEDIATE v_sql);
        LET v_config_cursor CURSOR FOR v_result;
        FOR config_record IN v_config_cursor DO
            v_dq_db_name := config_record.DQ_DB_NAME;
            v_dq_schema_name := config_record.DQ_SCHEMA_NAME;
            v_success_code := config_record.SUCCESS_CODE;
            v_failed_code := config_record.FAILED_CODE;
            v_execution_error := config_record.EXECUTION_ERROR;
            BREAK;
        END FOR;
        IF (v_dq_db_name IS NULL OR v_dq_schema_name IS NULL OR v_success_code IS NULL OR v_failed_code IS NULL OR v_execution_error IS NULL ) THEN
            v_error_message := ''Required Configurtion parameter is missing or NULL. Please check DQ_JOB_EXEC_CONFIG'';
            v_status_code := 400;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = ''Configuration Loading - failed'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        END IF;
        v_status_code := v_execution_error;
    EXCEPTION
        WHEN OTHER THEN
            v_error_message := ''Error loading configuration: '' || SQLERRM;
            v_status_code := 400;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
    END;
    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Config loaded'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 2. Parse and Validate the Rule Parameter
    v_step := ''RULE_PARSING'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', :v_input_rule_str);

    IF (RULE IS NULL) THEN
        v_error_message := ''Rule parameter is NULL'';
        v_status_code := v_execution_error;
        UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = :v_input_rule_str WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
        RETURN v_status_code;
    END IF;
    BEGIN
        v_batch_id := COALESCE(RULE:BATCH_ID::NUMBER, -1);
        v_run_id := COALESCE(RULE:DATASET_RUN_ID::NUMBER, -1);
        v_data_asset_id := COALESCE(RULE:DATASET_ID::NUMBER, -1);
        v_check_config_id := COALESCE(RULE:RULE_CONFIG_ID::NUMBER, -1);
        v_expectation_id := COALESCE(RULE:EXPECTATION_ID::NUMBER, -1);
        v_database_name := RULE:DATABASE_NAME::STRING;
        v_schema_name := RULE:SCHEMA_NAME::STRING;
        v_table_name := RULE:TABLE_NAME::STRING;
        v_run_name := RULE:RUN_NAME::STRING;
        v_data_asset_name := RULE:DATASET_NAME::STRING;
        v_expectation_name := RULE:EXPECTATION_NAME::STRING;
        v_dataset_type := RULE:DATASET_TYPE::STRING;
        v_sql_query := RULE:CUSTOM_SQL::STRING;

        v_kwargs_variant := PARSE_JSON(RULE:KWARGS);
        
        -- Handle column_set as either array or string format
        -- Use CS: prefix for case-sensitive columns, e.g., ["SCORE", "CS:TextCol"]
        LET column_set_raw VARIANT := v_kwargs_variant:column_set;
        IF (TYPEOF(column_set_raw) = ''VARCHAR'' OR TYPEOF(column_set_raw) = ''STRING'') THEN
            LET column_set_str STRING := column_set_raw::STRING;
            -- Simply convert single quotes to double quotes for JSON
            column_set_str := REPLACE(column_set_str, CHR(39), CHR(34));
            v_expected_columns := TRY_PARSE_JSON(column_set_str)::ARRAY;
            IF (v_expected_columns IS NULL) THEN
                v_error_message := ''Failed to parse column_set. Use format: ["COL1", "COL2", "CS:CaseSensitiveCol"]'';
                v_status_code := v_execution_error;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
                RETURN v_status_code;
            END IF;
        ELSE
            v_expected_columns := column_set_raw::ARRAY;
        END IF;
        
        v_exact_match := COALESCE(v_kwargs_variant:exact_match::BOOLEAN, TRUE);
        v_allowed_deviation := COALESCE(v_kwargs_variant:mostly::FLOAT, 1.0);
        v_procedure_name := COALESCE(RULE:PROCEDURE_NAME::STRING, ''SP_TABLE_COLUMNS_MATCH_SET_CHECK'');
        
        -- Validation
        IF (v_expected_columns IS NULL OR TYPEOF(v_expected_columns) != ''ARRAY'') THEN
            v_error_message := ''Required rule parameter column_set is missing or is not a valid array in KWARGS.'';
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = :v_input_rule_str WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        ELSEIF (UPPER(v_dataset_type) = ''TABLE'' AND (v_database_name IS NULL OR v_schema_name IS NULL OR v_table_name IS NULL)) THEN
            v_error_message := ''For DATASET_TYPE ''''TABLE'''', DATABASE_NAME, SCHEMA_NAME, and TABLE_NAME are required.'';
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = :v_input_rule_str WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        ELSEIF (UPPER(v_dataset_type) = ''QUERY'' AND v_sql_query IS NULL) THEN
            v_error_message := ''For DATASET_TYPE ''''QUERY'''', a CUSTOM_SQL is required.'';
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = :v_input_rule_str WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        ELSEIF (UPPER(v_dataset_type) != ''TABLE'' AND UPPER(v_dataset_type) != ''QUERY'') THEN
            v_error_message := ''Invalid value for DATASET_TYPE. Must be ''''TABLE'''' or ''''QUERY''''.'';
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = :v_input_rule_str WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        END IF;

    EXCEPTION
        WHEN OTHER THEN
            v_error_message := ''Error parsing rule parameter: '' || SQLERRM;
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = :v_input_rule_str WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
    END;
    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Input rule - parsing completed'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 3. Execute the Main Data Quality Check (Metadata Check)
    v_step := ''MAIN_METADATA_QUERY'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Retrieving actual column metadata'');

    IF (v_error_message IS NULL) THEN
        BEGIN
            -- Retrieve actual columns dynamically
            IF (UPPER(v_dataset_type) = ''TABLE'') THEN
                v_sql := ''SELECT ARRAY_AGG(COLUMN_NAME) WITHIN GROUP (ORDER BY ORDINAL_POSITION) AS ACTUAL_COLUMNS FROM '' ||
                        :v_database_name || ''.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '''''' || :v_schema_name || '''''' AND TABLE_NAME = '''''' || :v_table_name || '''''''';
            ELSE
                -- Execute a zero-row query to populate the result cache metadata
                v_sql_query := ''SELECT * FROM ('' || v_sql_query || '') LIMIT 0'';
                EXECUTE IMMEDIATE v_sql_query;
                -- Then query the result scan for column names and positions
                v_sql := ''SELECT ARRAY_AGG("name") WITHIN GROUP (ORDER BY "position") AS ACTUAL_COLUMNS FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))'';
            END IF;

            v_result := (EXECUTE IMMEDIATE v_sql);
            
            -- Use SELECT ... INTO with RESULT_SCAN to fetch the single column ARRAY result
            SELECT "ACTUAL_COLUMNS" INTO v_actual_columns FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
            
            v_actual_total := ARRAY_SIZE(v_actual_columns);

            IF (v_actual_columns IS NULL OR v_actual_total = 0) THEN
                v_error_message := ''The source asset (table or query) returned no columns. Check permissions or SQL syntax.'';
                v_status_code := v_execution_error;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
                RETURN v_status_code;
            END IF;
            
            -- Expected Set Processing: Use CS: prefix for case-sensitive columns
            -- Separate into case-sensitive and case-insensitive sets
            LET cs_expected ARRAY;
            LET ci_expected ARRAY;
            
            SELECT 
                ARRAY_AGG(CASE WHEN TRIM(value::STRING) LIKE ''CS:%'' THEN SUBSTR(TRIM(value::STRING), 4) END),
                ARRAY_AGG(CASE WHEN TRIM(value::STRING) NOT LIKE ''CS:%'' THEN UPPER(TRIM(value::STRING)) END)
            INTO cs_expected, ci_expected
            FROM TABLE(FLATTEN(input => :v_expected_columns));
            
            -- Remove NULLs from arrays
            cs_expected := COALESCE(ARRAY_COMPACT(cs_expected), ARRAY_CONSTRUCT());
            ci_expected := COALESCE(ARRAY_COMPACT(ci_expected), ARRAY_CONSTRUCT());
            
            -- Actual columns - separate into matching sets
            LET actual_original ARRAY;
            LET actual_upper ARRAY;
            
            SELECT 
                ARRAY_AGG(TRIM(value::STRING)),
                ARRAY_AGG(UPPER(TRIM(value::STRING)))
            INTO actual_original, actual_upper
            FROM TABLE(FLATTEN(input => :v_actual_columns));
            
            -- Check case-sensitive columns against original actual
            LET cs_missing ARRAY := ARRAY_EXCEPT(cs_expected, actual_original);
            -- Check case-insensitive columns against uppercased actual  
            LET ci_missing ARRAY := ARRAY_EXCEPT(ci_expected, actual_upper);
            
            -- Combine missing columns
            v_missing_columns := ARRAY_CAT(COALESCE(cs_missing, ARRAY_CONSTRUCT()), COALESCE(ci_missing, ARRAY_CONSTRUCT()));
            
            -- For unexpected columns: uppercase the CS expected for comparison
            LET cs_expected_upper ARRAY;
            SELECT ARRAY_AGG(UPPER(value::STRING)) INTO cs_expected_upper FROM TABLE(FLATTEN(input => :cs_expected));
            cs_expected_upper := COALESCE(cs_expected_upper, ARRAY_CONSTRUCT());
            
            -- All expected columns in uppercase for comparison
            LET all_expected_upper ARRAY := ARRAY_CAT(cs_expected_upper, ci_expected);
            
            -- Unexpected = actual columns (uppercased) not in expected (uppercased)
            v_unexpected_columns := ARRAY_EXCEPT(actual_upper, all_expected_upper);
            IF (v_unexpected_columns IS NULL) THEN
                v_unexpected_columns := ARRAY_CONSTRUCT();
            END IF;
            
            v_comparison_set_expected := ARRAY_CAT(cs_expected, ci_expected);
            v_comparison_set_actual := actual_original;
            
            v_total := ARRAY_SIZE(v_comparison_set_expected);
            v_unexpected := ARRAY_SIZE(v_missing_columns) + ARRAY_SIZE(v_unexpected_columns);
            
            -- Determine success
            LET missing_count INT := ARRAY_SIZE(v_missing_columns);
            LET unexpected_count INT := ARRAY_SIZE(v_unexpected_columns);
            
            v_success_flag := (missing_count = 0) AND (NOT v_exact_match OR unexpected_count = 0);
            v_percent := v_unexpected::FLOAT / NULLIF(v_total::FLOAT, 0);

            v_log_message := CASE WHEN v_success_flag THEN ''Column set check passed.'' ELSE ''Column set check failed due to mismatch.'' END;
            
            -- Final metrics update
            v_set_mismatches := ARRAY_SIZE(v_missing_columns) + ARRAY_SIZE(v_unexpected_columns);
            v_status_code := CASE WHEN v_success_flag THEN v_success_code ELSE v_failed_code END;
            
            -- Set v_observed_value (the actual list of columns)
            v_observed_value := v_actual_columns;
            
        EXCEPTION
            WHEN OTHER THEN
                v_error_message := ''Error in main metadata query execution: '' || SQLERRM;
                v_status_code := v_execution_error;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
                RETURN v_status_code;
        END;
    END IF;

    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 4 & 5. Failed record/key capture (Skipped - Not applicable for metadata checks)
    v_step := ''SKIPPING_FAILED_ROW_CAPTURE'';
    BEGIN
        v_log_message := ''Not applicable. Failed row/key capture is not performed for metadata checks.'';
        INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, END_TIMESTAMP, STATUS, LOG_MESSAGE)
        VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''COMPLETED'', :v_log_message);
        v_failed_records_table := ''No Failed Records'';
    EXCEPTION
        WHEN OTHER THEN
            v_error_message := ''Error in failed row capture step: '' || SQLERRM;
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
    END;

    ----------------------------------------------------------------------------------------------------
    -- 6. Insert Results into the DQ_RULE_RESULTS Table 
    v_step := ''INSERT_DQ_RESULTS_TABLE'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Loading results'');

    IF (v_error_message IS NULL) THEN
        BEGIN
            -- Build JSON objects directly as VARIANT types
            LET details_variant VARIANT := OBJECT_CONSTRUCT(
                ''expected_column_set'', v_expected_columns,
                ''exact_match'', v_exact_match
            );
            
            LET results_variant VARIANT := OBJECT_CONSTRUCT(
                ''expected_count'', v_total,
                ''actual_count'', v_actual_total,
                ''set_mismatch_count'', v_unexpected,
                ''missing_column_count'', ARRAY_SIZE(v_missing_columns),
                ''unexpected_column_count'', ARRAY_SIZE(v_unexpected_columns),
                ''missing_columns'', v_missing_columns,
                ''unexpected_columns'', v_unexpected_columns,
                ''failed_records_table'', ''Schema level Check - No Failed Records''
            );
            
            LET is_success BOOLEAN := (v_status_code = v_success_code);
            LET rule_variant VARIANT := RULE;
            LET observed_variant VARIANT := v_observed_value;
            
            -- Direct INSERT into the results table
            INSERT INTO DQ_RULE_RESULTS (
                BATCH_ID, DATASET_RUN_ID, DATASET_ID, RULE_CONFIG_ID, EXPECTATION_ID, RUN_NAME, RUN_TIMESTAMP, DATASET_NAME,
                EXPECATION_CONFIG, IS_SUCCESS, RESULTS, EXPECTATION_NAME, DETAILS, ELEMENT_COUNT,
                UNEXPECTED_COUNT, UNEXPECTED_PERCENT, UNEXPECTED_PERCENT_NONMISSING, UNEXPECTED_PERCENT_TOTAL,
                OBSERVED_VALUE, FAILED_ROWS
            )
            SELECT 
                :v_batch_id, :v_run_id, :v_data_asset_id, :v_check_config_id, :v_expectation_id, 
                :v_run_name, CURRENT_TIMESTAMP(), :v_data_asset_name,
                :rule_variant, :is_success, :results_variant, :v_expectation_name, :details_variant, :v_total,
                NULL, NULL, NULL, NULL, :observed_variant, NULL;
        EXCEPTION
            WHEN OTHER THEN
                v_error_message := ''Failed to insert into DQ_RULE_RESULTS: '' || SQLERRM;
                v_status_code := v_execution_error;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
                RETURN v_status_code;
        END;
    END IF;

    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Results is loaded'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    RETURN v_status_code;

EXCEPTION
    WHEN OTHER THEN
        BEGIN
            v_error_message := ''Global exception in step '' || COALESCE(:v_step, ''UNKNOWN'') || '': '' || SQLERRM;

            INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, END_TIMESTAMP, STATUS, ERROR_MESSAGE)
            VALUES (COALESCE(:v_run_id, -1), COALESCE(:v_check_config_id, -1), COALESCE(:v_procedure_name, ''SP_TABLE_COLUMNS_MATCH_SET_CHECK''), COALESCE(:v_step, ''UNKNOWN''), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''FAILED'', :v_error_message);

        EXCEPTION WHEN OTHER THEN NULL;
        END;
        RETURN COALESCE(v_execution_error, 400);
END;
';
