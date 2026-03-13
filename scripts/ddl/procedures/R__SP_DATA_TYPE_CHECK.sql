USE DATABASE {{ snowflake_database }};

USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE PROCEDURE "SP_DATA_TYPE_CHECK"("RULE" VARIANT)
RETURNS NUMBER(38, 0)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    v_sql TEXT;
    v_result RESULTSET;
    v_total INT DEFAULT 0;
    v_unexpected INT DEFAULT 0;
    v_percent FLOAT DEFAULT 0;
    v_status_code NUMBER;
    v_allowed_deviation FLOAT DEFAULT 1.0; -- Default for type check is 1.0 (100% compliance)
    v_error_message STRING;
    v_step STRING DEFAULT ''INITIALIZATION'';
    v_run_id NUMBER DEFAULT -1;
    v_column_nm STRING;
    v_type_expected STRING;
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
    v_batch_id NUMBER DEFAULT -1;
    v_procedure_name STRING DEFAULT ''SP_DATA_TYPE_CHECK'';
    v_input_rule_str STRING;
    v_log_message STRING;
    
    -- Variables specific to this check
    v_kwargs_variant VARIANT;
    v_dataset_type STRING;
    v_sql_query STRING;
    v_dimension STRING;
    v_failed_records_table STRING; 
    v_from_clause STRING;
    v_where_clause_condition STRING;
    v_failed_rows_cnt_limit NUMBER;
    v_failed_rows_threshold INT DEFAULT 10000000;
    v_rows_inserted NUMBER DEFAULT 0;
    v_stage_path STRING;
    
    -- Variables for failed row keys enhancement
    v_key_column_names STRING;
    v_pk_column_names STRING;
    v_ck_column_names STRING;
    v_key_construct_expr STRING;
    v_key_parts_list STRING;
    v_observed_value VARIANT;

    -- DYNAMIC TABLE VARIABLES (NEW)
    v_clean_dataset_name STRING;
    v_full_target_table_name STRING;

    -- Column profile metrics for error records
    v_column_profile VARIANT;
    v_value_counts VARIANT;
    v_col_unique_count NUMBER;
    v_col_unique_percent FLOAT;

    -- Missing count variables
    v_missing_count NUMBER DEFAULT 0;
    v_missing_percent FLOAT DEFAULT 0;
    v_unexpected_percent_total FLOAT DEFAULT 0;
    v_unexpected_percent_nonmissing FLOAT DEFAULT 0;

    -- TYPE DETECTION VARIABLE (for same-type comparison handling)
    v_actual_column_type STRING;
    v_type_match_flag BOOLEAN DEFAULT FALSE;
    v_unsupported_conversion_flag BOOLEAN DEFAULT FALSE;

BEGIN
    v_input_rule_str := TO_VARCHAR(RULE);

    ----------------------------------------------------------------------------------------------------
    -- 1. Load Configuration
    ----------------------------------------------------------------------------------------------------
    
    v_step := ''CONFIG_LOADING'';
    v_run_id := COALESCE(RULE:DATASET_RUN_ID::NUMBER, -1);
    v_check_config_id := COALESCE(RULE:RULE_CONFIG_ID::NUMBER, -1);
    

    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Loading configuration'');

    BEGIN
        v_sql := ''SELECT DQ_DB_NAME, DQ_SCHEMA_NAME, SUCCESS_CODE, FAILED_CODE, EXECUTION_ERROR FROM DQ_JOB_EXEC_CONFIG LIMIT 1'';
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

    ---
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
        -- Standard Parameter Loading
        v_batch_id := COALESCE(RULE:BATCH_ID::NUMBER, -1);
        v_run_id := COALESCE(RULE:DATASET_RUN_ID::NUMBER, -1);
        v_check_config_id := COALESCE(RULE:RULE_CONFIG_ID::NUMBER, -1);
        v_data_asset_id := COALESCE(RULE:DATASET_ID::NUMBER, -1);
        v_expectation_id := COALESCE(RULE:EXPECTATION_ID::NUMBER, -1);
        v_run_name := RULE:RUN_NAME::STRING;
        v_data_asset_name := RULE:DATASET_NAME::STRING;
        v_expectation_name := RULE:EXPECTATION_NAME::STRING;
        v_database_name := RULE:DATABASE_NAME::STRING;
        v_schema_name := RULE:SCHEMA_NAME::STRING;
        v_table_name := RULE:TABLE_NAME::STRING;
        v_dataset_type := UPPER(RULE:DATASET_TYPE::STRING);
        v_sql_query := RULE:CUSTOM_SQL::STRING;
        v_dimension := RULE:DIMENSION;
        v_key_column_names := RULE:KEY_COLUMN_NAMES::STRING;
        
        -- KWARGS Parsing and Extraction
        v_kwargs_variant := PARSE_JSON(RULE:KWARGS);
        v_column_nm := v_kwargs_variant:column::STRING;
        v_type_expected := UPPER(v_kwargs_variant:type_::STRING); 
        v_allowed_deviation := COALESCE(v_kwargs_variant:mostly::FLOAT, 1.0);
        v_failed_rows_cnt_limit := v_kwargs_variant:failed_row_count::NUMBER;
        
        -- Validation Checks
        IF (v_column_nm IS NULL OR v_type_expected IS NULL) THEN
            v_error_message := ''Required rule parameters COLUMN (or KWARGS:column) or TYPE_ (or KWARGS:type_) are missing or NULL.'';
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = :v_input_rule_str WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        ELSEIF (v_dataset_type = ''TABLE'' AND (v_database_name IS NULL OR v_schema_name IS NULL OR v_table_name IS NULL)) THEN
            v_error_message := ''For DATASET_TYPE ''''TABLE'''', DATABASE_NAME, SCHEMA_NAME, and TABLE_NAME are required.'';
            v_status_code := v_execution_error;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message, LOG_MESSAGE = :v_input_rule_str WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        ELSEIF (v_dataset_type = ''QUERY'' AND v_sql_query IS NULL) THEN
            v_error_message := ''For DATASET_TYPE ''''QUERY'''', a CUSTOM_SQL is required.'';
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
    -- 3. Execute the Main Data Quality Check Query
    ----------------------------------------------------------------------------------------------------

    v_step := ''MAIN_QUERY'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Starting data type validation query'');

    -- Dynamically build the FROM clause based on dataset_type
    IF (v_dataset_type = ''QUERY'') THEN
        v_from_clause := ''('' || v_sql_query || '') AS custom_query_source'';
    ELSE
        v_from_clause := ''"'' || v_database_name || ''"."'' || v_schema_name || ''"."'' || v_table_name || ''"'';
    END IF;

    -- Check if column type matches expected type (to avoid TRY_CAST same-type error)
    IF (v_dataset_type = ''TABLE'') THEN
        BEGIN
            v_sql := ''SELECT DATA_TYPE FROM "'' || v_database_name || ''".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '''''' || v_schema_name || '''''' AND TABLE_NAME = '''''' || v_table_name || '''''' AND UPPER(COLUMN_NAME) = '''''' || UPPER(v_column_nm) || '''''''';
            v_result := (EXECUTE IMMEDIATE v_sql);
            LET v_type_cursor CURSOR FOR v_result;
            FOR type_record IN v_type_cursor DO
                v_actual_column_type := UPPER(type_record.DATA_TYPE);
                BREAK;
            END FOR;
            
            -- Check for type compatibility: same type OR safe conversions where TRY_CAST fails
            IF (v_actual_column_type LIKE ''%'' || v_type_expected || ''%'' OR v_type_expected LIKE ''%'' || SPLIT_PART(v_actual_column_type, ''('', 1) || ''%'') THEN
                v_type_match_flag := TRUE;
            -- NUMBER/INT/FLOAT to VARCHAR/STRING always succeeds (TRY_CAST not supported but conversion always works)
            ELSEIF ((v_actual_column_type LIKE ''NUMBER%'' OR v_actual_column_type LIKE ''INT%'' OR v_actual_column_type LIKE ''FLOAT%'' OR v_actual_column_type LIKE ''DECIMAL%'' OR v_actual_column_type LIKE ''DOUBLE%'') 
                    AND (v_type_expected IN (''VARCHAR'', ''STRING'', ''TEXT'', ''CHAR''))) THEN
                v_type_match_flag := TRUE;
            -- NUMBER to DATE/TIMESTAMP: TRY_CAST not supported - mark as failed with 100% failure rate
            ELSEIF ((v_actual_column_type LIKE ''NUMBER%'' OR v_actual_column_type LIKE ''INT%'' OR v_actual_column_type LIKE ''FLOAT%'' OR v_actual_column_type LIKE ''DECIMAL%'' OR v_actual_column_type LIKE ''DOUBLE%'') 
                    AND (v_type_expected IN (''DATE'', ''TIMESTAMP'', ''TIMESTAMP_NTZ'', ''TIMESTAMP_LTZ'', ''TIMESTAMP_TZ'', ''TIME''))) THEN
                -- Get total count for the failure metrics
                v_sql := ''SELECT COUNT(*) AS total_count FROM '' || v_from_clause;
                v_result := (EXECUTE IMMEDIATE v_sql);
                LET v_unsupported_cursor CURSOR FOR v_result;
                FOR unsupported_record IN v_unsupported_cursor DO
                    v_total := COALESCE(unsupported_record.total_count, 0);
                    BREAK;
                END FOR;
                v_unexpected := v_total;
                v_percent := 1.0;
                v_status_code := v_failed_code;
                v_log_message := ''Column '' || v_column_nm || '' has type '' || v_actual_column_type || ''. TRY_CAST does not support conversion to '' || v_type_expected || ''. All rows marked as failed.'';
                v_failed_records_table := ''Unsupported conversion - all rows failed'';
                v_unsupported_conversion_flag := TRUE;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            -- DATE/TIMESTAMP to VARCHAR/STRING always succeeds
            ELSEIF ((v_actual_column_type LIKE ''DATE%'' OR v_actual_column_type LIKE ''TIMESTAMP%'' OR v_actual_column_type LIKE ''TIME%'') 
                    AND (v_type_expected IN (''VARCHAR'', ''STRING'', ''TEXT'', ''CHAR''))) THEN
                v_type_match_flag := TRUE;
            -- DATE/TIMESTAMP to NUMBER: TRY_CAST not supported - mark as failed
            ELSEIF ((v_actual_column_type LIKE ''DATE%'' OR v_actual_column_type LIKE ''TIMESTAMP%'' OR v_actual_column_type LIKE ''TIME%'') 
                    AND (v_type_expected IN (''NUMBER'', ''INT'', ''INTEGER'', ''FLOAT'', ''DECIMAL'', ''DOUBLE''))) THEN
                v_sql := ''SELECT COUNT(*) AS total_count FROM '' || v_from_clause;
                v_result := (EXECUTE IMMEDIATE v_sql);
                LET v_dt_num_cursor CURSOR FOR v_result;
                FOR dt_num_record IN v_dt_num_cursor DO
                    v_total := COALESCE(dt_num_record.total_count, 0);
                    BREAK;
                END FOR;
                v_unexpected := v_total;
                v_percent := 1.0;
                v_status_code := v_failed_code;
                v_log_message := ''Column '' || v_column_nm || '' has type '' || v_actual_column_type || ''. TRY_CAST does not support conversion to '' || v_type_expected || ''. All rows marked as failed.'';
                v_failed_records_table := ''Unsupported conversion - all rows failed'';
                v_unsupported_conversion_flag := TRUE;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            -- DATE/TIMESTAMP to BOOLEAN: TRY_CAST not supported - mark as failed
            ELSEIF ((v_actual_column_type LIKE ''DATE%'' OR v_actual_column_type LIKE ''TIMESTAMP%'' OR v_actual_column_type LIKE ''TIME%'') 
                    AND (v_type_expected = ''BOOLEAN'')) THEN
                v_sql := ''SELECT COUNT(*) AS total_count FROM '' || v_from_clause;
                v_result := (EXECUTE IMMEDIATE v_sql);
                LET v_dt_bool_cursor CURSOR FOR v_result;
                FOR dt_bool_record IN v_dt_bool_cursor DO
                    v_total := COALESCE(dt_bool_record.total_count, 0);
                    BREAK;
                END FOR;
                v_unexpected := v_total;
                v_percent := 1.0;
                v_status_code := v_failed_code;
                v_log_message := ''Column '' || v_column_nm || '' has type '' || v_actual_column_type || ''. TRY_CAST does not support conversion to BOOLEAN. All rows marked as failed.'';
                v_failed_records_table := ''Unsupported conversion - all rows failed'';
                v_unsupported_conversion_flag := TRUE;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            -- BOOLEAN to VARCHAR/STRING always succeeds
            ELSEIF (v_actual_column_type = ''BOOLEAN'' AND (v_type_expected IN (''VARCHAR'', ''STRING'', ''TEXT'', ''CHAR''))) THEN
                v_type_match_flag := TRUE;
            -- BOOLEAN to NUMBER: TRY_CAST not supported - mark as failed
            ELSEIF (v_actual_column_type = ''BOOLEAN'' AND (v_type_expected IN (''NUMBER'', ''INT'', ''INTEGER'', ''FLOAT'', ''DECIMAL'', ''DOUBLE''))) THEN
                v_sql := ''SELECT COUNT(*) AS total_count FROM '' || v_from_clause;
                v_result := (EXECUTE IMMEDIATE v_sql);
                LET v_bool_num_cursor CURSOR FOR v_result;
                FOR bool_num_record IN v_bool_num_cursor DO
                    v_total := COALESCE(bool_num_record.total_count, 0);
                    BREAK;
                END FOR;
                v_unexpected := v_total;
                v_percent := 1.0;
                v_status_code := v_failed_code;
                v_log_message := ''Column '' || v_column_nm || '' has type BOOLEAN. TRY_CAST does not support conversion to '' || v_type_expected || ''. All rows marked as failed.'';
                v_failed_records_table := ''Unsupported conversion - all rows failed'';
                v_unsupported_conversion_flag := TRUE;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            -- BOOLEAN to DATE/TIMESTAMP: TRY_CAST not supported - mark as failed
            ELSEIF (v_actual_column_type = ''BOOLEAN'' AND (v_type_expected IN (''DATE'', ''TIMESTAMP'', ''TIMESTAMP_NTZ'', ''TIMESTAMP_LTZ'', ''TIMESTAMP_TZ'', ''TIME''))) THEN
                v_sql := ''SELECT COUNT(*) AS total_count FROM '' || v_from_clause;
                v_result := (EXECUTE IMMEDIATE v_sql);
                LET v_bool_dt_cursor CURSOR FOR v_result;
                FOR bool_dt_record IN v_bool_dt_cursor DO
                    v_total := COALESCE(bool_dt_record.total_count, 0);
                    BREAK;
                END FOR;
                v_unexpected := v_total;
                v_percent := 1.0;
                v_status_code := v_failed_code;
                v_log_message := ''Column '' || v_column_nm || '' has type BOOLEAN. TRY_CAST does not support conversion to '' || v_type_expected || ''. All rows marked as failed.'';
                v_failed_records_table := ''Unsupported conversion - all rows failed'';
                v_unsupported_conversion_flag := TRUE;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            -- NUMBER to BOOLEAN: TRY_CAST not supported - mark as failed
            ELSEIF ((v_actual_column_type LIKE ''NUMBER%'' OR v_actual_column_type LIKE ''INT%'' OR v_actual_column_type LIKE ''FLOAT%'' OR v_actual_column_type LIKE ''DECIMAL%'' OR v_actual_column_type LIKE ''DOUBLE%'') 
                    AND (v_type_expected = ''BOOLEAN'')) THEN
                v_sql := ''SELECT COUNT(*) AS total_count FROM '' || v_from_clause;
                v_result := (EXECUTE IMMEDIATE v_sql);
                LET v_num_bool_cursor CURSOR FOR v_result;
                FOR num_bool_record IN v_num_bool_cursor DO
                    v_total := COALESCE(num_bool_record.total_count, 0);
                    BREAK;
                END FOR;
                v_unexpected := v_total;
                v_percent := 1.0;
                v_status_code := v_failed_code;
                v_log_message := ''Column '' || v_column_nm || '' has type '' || v_actual_column_type || ''. TRY_CAST does not support conversion to BOOLEAN. All rows marked as failed.'';
                v_failed_records_table := ''Unsupported conversion - all rows failed'';
                v_unsupported_conversion_flag := TRUE;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            END IF;
        EXCEPTION
            WHEN OTHER THEN
                v_type_match_flag := FALSE;
        END;
    ELSEIF (v_dataset_type = ''QUERY'') THEN
        BEGIN
            -- For QUERY type, get column type using DESCRIBE RESULT
            v_sql := ''SELECT * FROM ('' || v_sql_query || '') WHERE 1=0'';
            EXECUTE IMMEDIATE v_sql;
            
            -- Use DESCRIBE RESULT to get column metadata
            EXECUTE IMMEDIATE ''DESCRIBE RESULT LAST_QUERY_ID()'';
            v_sql := ''SELECT "type" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE UPPER("name") = '''''' || UPPER(v_column_nm) || '''''''';
            v_result := (EXECUTE IMMEDIATE v_sql);
            LET v_query_type_cursor CURSOR FOR v_result;
            FOR query_type_record IN v_query_type_cursor DO
                v_actual_column_type := UPPER(query_type_record."type");
                BREAK;
            END FOR;
            
            -- Apply same type matching logic as TABLE type
            IF (v_actual_column_type LIKE ''%'' || v_type_expected || ''%'' OR v_type_expected LIKE ''%'' || SPLIT_PART(v_actual_column_type, ''('', 1) || ''%'') THEN
                v_type_match_flag := TRUE;
            ELSEIF ((v_actual_column_type LIKE ''NUMBER%'' OR v_actual_column_type LIKE ''INT%'' OR v_actual_column_type LIKE ''FLOAT%'' OR v_actual_column_type LIKE ''DECIMAL%'' OR v_actual_column_type LIKE ''DOUBLE%'') 
                    AND (v_type_expected IN (''VARCHAR'', ''STRING'', ''TEXT'', ''CHAR''))) THEN
                v_type_match_flag := TRUE;
            ELSEIF ((v_actual_column_type LIKE ''NUMBER%'' OR v_actual_column_type LIKE ''INT%'' OR v_actual_column_type LIKE ''FLOAT%'' OR v_actual_column_type LIKE ''DECIMAL%'' OR v_actual_column_type LIKE ''DOUBLE%'') 
                    AND (v_type_expected IN (''DATE'', ''TIMESTAMP'', ''TIMESTAMP_NTZ'', ''TIMESTAMP_LTZ'', ''TIMESTAMP_TZ'', ''TIME''))) THEN
                v_sql := ''SELECT COUNT(*) AS total_count FROM '' || v_from_clause;
                v_result := (EXECUTE IMMEDIATE v_sql);
                LET v_q_unsupported_cursor CURSOR FOR v_result;
                FOR q_unsupported_record IN v_q_unsupported_cursor DO
                    v_total := COALESCE(q_unsupported_record.total_count, 0);
                    BREAK;
                END FOR;
                v_unexpected := v_total;
                v_percent := 1.0;
                v_status_code := v_failed_code;
                v_log_message := ''Column '' || v_column_nm || '' has type '' || v_actual_column_type || ''. TRY_CAST does not support conversion to '' || v_type_expected || ''. All rows marked as failed.'';
                v_failed_records_table := ''Unsupported conversion - all rows failed'';
                v_unsupported_conversion_flag := TRUE;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            ELSEIF ((v_actual_column_type LIKE ''DATE%'' OR v_actual_column_type LIKE ''TIMESTAMP%'' OR v_actual_column_type LIKE ''TIME%'') 
                    AND (v_type_expected IN (''VARCHAR'', ''STRING'', ''TEXT'', ''CHAR''))) THEN
                v_type_match_flag := TRUE;
            ELSEIF ((v_actual_column_type LIKE ''DATE%'' OR v_actual_column_type LIKE ''TIMESTAMP%'' OR v_actual_column_type LIKE ''TIME%'') 
                    AND (v_type_expected IN (''NUMBER'', ''INT'', ''INTEGER'', ''FLOAT'', ''DECIMAL'', ''DOUBLE''))) THEN
                v_sql := ''SELECT COUNT(*) AS total_count FROM '' || v_from_clause;
                v_result := (EXECUTE IMMEDIATE v_sql);
                LET v_q_dt_num_cursor CURSOR FOR v_result;
                FOR q_dt_num_record IN v_q_dt_num_cursor DO
                    v_total := COALESCE(q_dt_num_record.total_count, 0);
                    BREAK;
                END FOR;
                v_unexpected := v_total;
                v_percent := 1.0;
                v_status_code := v_failed_code;
                v_log_message := ''Column '' || v_column_nm || '' has type '' || v_actual_column_type || ''. TRY_CAST does not support conversion to '' || v_type_expected || ''. All rows marked as failed.'';
                v_failed_records_table := ''Unsupported conversion - all rows failed'';
                v_unsupported_conversion_flag := TRUE;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            ELSEIF ((v_actual_column_type LIKE ''DATE%'' OR v_actual_column_type LIKE ''TIMESTAMP%'' OR v_actual_column_type LIKE ''TIME%'') 
                    AND (v_type_expected = ''BOOLEAN'')) THEN
                v_sql := ''SELECT COUNT(*) AS total_count FROM '' || v_from_clause;
                v_result := (EXECUTE IMMEDIATE v_sql);
                LET v_q_dt_bool_cursor CURSOR FOR v_result;
                FOR q_dt_bool_record IN v_q_dt_bool_cursor DO
                    v_total := COALESCE(q_dt_bool_record.total_count, 0);
                    BREAK;
                END FOR;
                v_unexpected := v_total;
                v_percent := 1.0;
                v_status_code := v_failed_code;
                v_log_message := ''Column '' || v_column_nm || '' has type '' || v_actual_column_type || ''. TRY_CAST does not support conversion to BOOLEAN. All rows marked as failed.'';
                v_failed_records_table := ''Unsupported conversion - all rows failed'';
                v_unsupported_conversion_flag := TRUE;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            ELSEIF (v_actual_column_type = ''BOOLEAN'' AND (v_type_expected IN (''VARCHAR'', ''STRING'', ''TEXT'', ''CHAR''))) THEN
                v_type_match_flag := TRUE;
            ELSEIF (v_actual_column_type = ''BOOLEAN'' AND (v_type_expected IN (''NUMBER'', ''INT'', ''INTEGER'', ''FLOAT'', ''DECIMAL'', ''DOUBLE''))) THEN
                v_sql := ''SELECT COUNT(*) AS total_count FROM '' || v_from_clause;
                v_result := (EXECUTE IMMEDIATE v_sql);
                LET v_q_bool_num_cursor CURSOR FOR v_result;
                FOR q_bool_num_record IN v_q_bool_num_cursor DO
                    v_total := COALESCE(q_bool_num_record.total_count, 0);
                    BREAK;
                END FOR;
                v_unexpected := v_total;
                v_percent := 1.0;
                v_status_code := v_failed_code;
                v_log_message := ''Column '' || v_column_nm || '' has type BOOLEAN. TRY_CAST does not support conversion to '' || v_type_expected || ''. All rows marked as failed.'';
                v_failed_records_table := ''Unsupported conversion - all rows failed'';
                v_unsupported_conversion_flag := TRUE;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            ELSEIF (v_actual_column_type = ''BOOLEAN'' AND (v_type_expected IN (''DATE'', ''TIMESTAMP'', ''TIMESTAMP_NTZ'', ''TIMESTAMP_LTZ'', ''TIMESTAMP_TZ'', ''TIME''))) THEN
                v_sql := ''SELECT COUNT(*) AS total_count FROM '' || v_from_clause;
                v_result := (EXECUTE IMMEDIATE v_sql);
                LET v_q_bool_dt_cursor CURSOR FOR v_result;
                FOR q_bool_dt_record IN v_q_bool_dt_cursor DO
                    v_total := COALESCE(q_bool_dt_record.total_count, 0);
                    BREAK;
                END FOR;
                v_unexpected := v_total;
                v_percent := 1.0;
                v_status_code := v_failed_code;
                v_log_message := ''Column '' || v_column_nm || '' has type BOOLEAN. TRY_CAST does not support conversion to '' || v_type_expected || ''. All rows marked as failed.'';
                v_failed_records_table := ''Unsupported conversion - all rows failed'';
                v_unsupported_conversion_flag := TRUE;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            ELSEIF ((v_actual_column_type LIKE ''NUMBER%'' OR v_actual_column_type LIKE ''INT%'' OR v_actual_column_type LIKE ''FLOAT%'' OR v_actual_column_type LIKE ''DECIMAL%'' OR v_actual_column_type LIKE ''DOUBLE%'') 
                    AND (v_type_expected = ''BOOLEAN'')) THEN
                v_sql := ''SELECT COUNT(*) AS total_count FROM '' || v_from_clause;
                v_result := (EXECUTE IMMEDIATE v_sql);
                LET v_q_num_bool_cursor CURSOR FOR v_result;
                FOR q_num_bool_record IN v_q_num_bool_cursor DO
                    v_total := COALESCE(q_num_bool_record.total_count, 0);
                    BREAK;
                END FOR;
                v_unexpected := v_total;
                v_percent := 1.0;
                v_status_code := v_failed_code;
                v_log_message := ''Column '' || v_column_nm || '' has type '' || v_actual_column_type || ''. TRY_CAST does not support conversion to BOOLEAN. All rows marked as failed.'';
                v_failed_records_table := ''Unsupported conversion - all rows failed'';
                v_unsupported_conversion_flag := TRUE;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            END IF;
        EXCEPTION
            WHEN OTHER THEN
                v_type_match_flag := FALSE;
        END;
    END IF;

    -- If types match, auto-pass without TRY_CAST
    -- If unsupported conversion, skip to results (already set as failed)
    IF (v_unsupported_conversion_flag = TRUE) THEN
        -- Skip to step 6 - results already set
        NULL;
    ELSEIF (v_type_match_flag = TRUE) THEN
        BEGIN
            v_sql := ''SELECT COUNT(*) AS total_count FROM '' || v_from_clause;
            v_result := (EXECUTE IMMEDIATE v_sql);
            LET v_count_cursor CURSOR FOR v_result;
            FOR count_record IN v_count_cursor DO
                v_total := COALESCE(count_record.total_count, 0);
                BREAK;
            END FOR;
            v_unexpected := 0;
            v_percent := 0;
            v_status_code := v_success_code;
            v_log_message := ''Column '' || v_column_nm || '' has type '' || v_actual_column_type || '' converting to '' || v_type_expected || ''. Safe conversion - check auto-passed.'';
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
        EXCEPTION
            WHEN OTHER THEN
                v_error_message := ''Error getting row count for type-matched column: '' || SQLERRM;
                v_status_code := v_execution_error;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
                RETURN v_status_code;
        END;
    ELSE
        -- Define the failure condition: Value is NOT NULL AND TRY_CAST fails.
        v_where_clause_condition := ''"'' || v_column_nm || ''" IS NOT NULL AND TRIM("'' || v_column_nm || ''"::STRING) <> '''''''' AND TRY_CAST("'' || v_column_nm || ''" AS '' || v_type_expected || '') IS NULL'';

        v_sql := ''SELECT 
                     COUNT(*) AS total_count,
                     COUNT_IF("'' || v_column_nm || ''" IS NULL OR TRIM("'' || v_column_nm || ''"::STRING) = '''''''') AS missing_count,
                     COUNT_IF('' || v_where_clause_condition || '') AS unexpected_count
                   FROM '' || v_from_clause;

        BEGIN
            v_result := (EXECUTE IMMEDIATE v_sql);
            LET v_cursor CURSOR FOR v_result;
            
            FOR record IN v_cursor DO
                v_total := COALESCE(record.total_count, 0);
                v_missing_count := COALESCE(record.missing_count, 0);
                v_unexpected := COALESCE(record.unexpected_count, 0);
                BREAK;
            END FOR;

            v_missing_percent := CASE WHEN v_total = 0 THEN 0 ELSE (v_missing_count::FLOAT / v_total) END;
            v_percent := CASE WHEN v_total = 0 THEN 0 ELSE (v_unexpected::FLOAT / v_total) END;
            v_unexpected_percent_total := v_percent;
            v_unexpected_percent_nonmissing := CASE WHEN (v_total - v_missing_count) = 0 THEN 0 ELSE (v_unexpected::FLOAT / (v_total - v_missing_count)) END;
            
            -- Check against the allowed deviation (mostly)
            v_status_code := CASE WHEN v_percent <= (1 - v_allowed_deviation) THEN v_success_code ELSE v_failed_code END;
            
        EXCEPTION
            WHEN OTHER THEN
                v_error_message := ''Error in main query execution: '' || SQLERRM;
                v_status_code := v_execution_error;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
                RETURN v_status_code;
        END;
    END IF;

    IF (v_type_match_flag = FALSE) THEN
        v_log_message := ''Column '' || v_column_nm || '' expected '' || v_type_expected || '' (mostly >= '' || v_allowed_deviation*100 || ''%). Found '' || v_unexpected || '' unexpected out of '' || v_total || '' rows ('' || ROUND(v_percent*100, 2) || ''%).'';
        UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
    END IF;

    ----------------------------------------------------------------------------------------------------
    -- 4. Capture Failed Row Keys
    ----------------------------------------------------------------------------------------------------

    v_step := ''CAPTURE_FAILED_KEYS'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Processing failed row keys'');
    
    IF (v_unexpected > 0) THEN
        BEGIN
            -- Key lookup logic from DQ_DATASET (only if KEY_COLUMN_NAMES wasn''t provided in the RULE variant)
            IF (v_key_column_names IS NULL) THEN
                v_sql := ''SELECT PRIMARY_KEY_COLUMNS, CANDIDATE_KEY_COLUMNS FROM DQ_DATASET WHERE DATASET_ID = '' || v_data_asset_id;
                v_result := (EXECUTE IMMEDIATE v_sql);
                LET v_pk_cursor CURSOR FOR v_result;
                FOR pk_record IN v_pk_cursor DO
                    v_pk_column_names := ARRAY_TO_STRING(PARSE_JSON(pk_record.PRIMARY_KEY_COLUMNS):primary_key, '', '');
                    v_ck_column_names := pk_record.CANDIDATE_KEY_COLUMNS;
                    BREAK;
                END FOR;
            
                IF (v_pk_column_names IS NOT NULL AND TRIM(v_pk_column_names) != '''') THEN
                    v_key_column_names := v_pk_column_names;
                    v_log_message := ''Using Primary Key (''||v_key_column_names||'') for failed row key capture.'';
                ELSEIF (v_ck_column_names IS NOT NULL AND TRIM(v_ck_column_names) != '''') THEN
                    v_key_column_names := v_ck_column_names;
                    v_log_message := ''Primary key not found, using Candidate Key (''||v_key_column_names||'') for failed row key capture.'';
                ELSE
                    v_key_column_names := NULL;
                    v_log_message := ''No Primary or Candidate Key found for the dataset. Skipping failed key capture.'';
                END IF;
            END IF;
            
            IF (v_key_column_names IS NOT NULL) THEN
                -- Construct the OBJECT_CONSTRUCT string for the key columns
                SELECT LISTAGG('''''''' || TRIM(value) || '''''''' || '', '' || TRIM(value), '', '') WITHIN GROUP (ORDER BY seq)
                INTO v_key_parts_list
                FROM TABLE(SPLIT_TO_TABLE(:v_key_column_names, '',''));
                v_key_construct_expr := ''OBJECT_CONSTRUCT('' || v_key_parts_list || '')'';
                
                -- For unsupported conversions, capture ALL rows (no WHERE clause)
                IF (v_unsupported_conversion_flag = TRUE) THEN
                    v_sql := ''INSERT INTO DQ_FAILED_ROW_KEYS (DATASET_RUN_ID, RULE_CONFIG_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, FAILED_KEY) SELECT '' ||
                             v_run_id || '', '' || v_check_config_id || '', '' ||
                             '''''''' || COALESCE(v_database_name, ''N/A'') || '''''', '' || 
                             '''''''' || COALESCE(v_schema_name, ''N/A'') || '''''', '' ||   
                             '''''''' || COALESCE(v_table_name, ''N/A'') || '''''', '' ||   
                             v_key_construct_expr ||
                             '' FROM '' || v_from_clause;
                ELSE
                    v_sql := ''INSERT INTO DQ_FAILED_ROW_KEYS (DATASET_RUN_ID, RULE_CONFIG_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, FAILED_KEY) SELECT '' ||
                             v_run_id || '', '' || v_check_config_id || '', '' ||
                             '''''''' || COALESCE(v_database_name, ''N/A'') || '''''', '' || 
                             '''''''' || COALESCE(v_schema_name, ''N/A'') || '''''', '' ||   
                             '''''''' || COALESCE(v_table_name, ''N/A'') || '''''', '' ||   
                             v_key_construct_expr ||
                             '' FROM '' || v_from_clause || '' WHERE '' || v_where_clause_condition;
                END IF;
                
                EXECUTE IMMEDIATE v_sql;
                v_rows_inserted := SQLROWCOUNT;
                v_log_message := v_rows_inserted || '' keys of failed rows captured. '' || v_log_message;
            END IF;
        EXCEPTION
            WHEN OTHER THEN
                v_error_message := ''Error capturing failed row keys: '' || SQLERRM;
                v_status_code := v_execution_error;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
                RETURN v_status_code;
        END;
    ELSE
        v_log_message := ''No failed rows found, skipping failed key capture.'';
    END IF;
    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 5. Handle and Log Failed Records (UPDATED: Structured Failure Table)
    ----------------------------------------------------------------------------------------------------

    v_step := ''INSERT_FAILED_RECORDS'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Processing failed records into structured table'');

    IF (v_unexpected > 0) THEN
        BEGIN
            -- 1. Construct standard failure table name: <DATASET_NAME>_DQ_FAILURE
            -- Remove special characters from dataset name to ensure valid table identifier
            v_clean_dataset_name := REGEXP_REPLACE(v_data_asset_name, ''[^a-zA-Z0-9]'', ''_'');
            v_failed_records_table := v_clean_dataset_name || ''_DQ_FAILURE'';
            v_full_target_table_name := ''"'' || v_dq_db_name || ''"."'' || v_dq_schema_name || ''"."'' || v_failed_records_table || ''"'';

            -- 2. Create Failure Table IF NOT EXISTS
            -- We replicate the source schema (from v_from_clause) and prepend audit columns
            v_sql := ''CREATE TABLE IF NOT EXISTS '' || v_full_target_table_name || '' AS '' ||
                     ''SELECT '' ||
                     v_run_id || ''::NUMBER(38,0) AS DATASET_RUN_ID, '' ||
                     v_data_asset_id || ''::NUMBER(38,0) AS DATASET_ID, '' ||
                     v_check_config_id || ''::NUMBER(38,0) AS RULE_CONFIG_ID, '' ||
                     ''CURRENT_TIMESTAMP()::TIMESTAMP_LTZ AS DQ_LOAD_TIMESTAMP, '' ||
                     '' * FROM '' || v_from_clause || '' WHERE 1=0'';
            
            EXECUTE IMMEDIATE v_sql;

            -- 3. Insert Failed Records
            -- For unsupported conversions, insert ALL rows (no WHERE clause)
            IF (v_unsupported_conversion_flag = TRUE) THEN
                v_sql := ''INSERT INTO '' || v_full_target_table_name || '' '' ||
                         ''SELECT '' ||
                         v_run_id || '', '' ||
                         v_data_asset_id || '', '' ||
                         v_check_config_id || '', '' ||
                         ''CURRENT_TIMESTAMP(), '' ||
                         '' * FROM '' || v_from_clause ||
                         CASE WHEN v_failed_rows_cnt_limit > 0 THEN '' LIMIT '' || v_failed_rows_cnt_limit ELSE '''' END;
            ELSE
                v_sql := ''INSERT INTO '' || v_full_target_table_name || '' '' ||
                         ''SELECT '' ||
                         v_run_id || '', '' ||
                         v_data_asset_id || '', '' ||
                         v_check_config_id || '', '' ||
                         ''CURRENT_TIMESTAMP(), '' ||
                         '' * FROM '' || v_from_clause || 
                         '' WHERE '' || v_where_clause_condition ||
                         CASE WHEN v_failed_rows_cnt_limit > 0 THEN '' LIMIT '' || v_failed_rows_cnt_limit ELSE '''' END;
            END IF;

            EXECUTE IMMEDIATE v_sql;
            v_rows_inserted := SQLROWCOUNT;
            
            v_log_message := v_rows_inserted || '' rows inserted into structured failure table: '' || v_failed_records_table;

        EXCEPTION
            WHEN OTHER THEN
                v_error_message := ''Failed to process structured failed records: '' || SQLERRM;
                v_status_code := v_execution_error;
                UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
                RETURN v_status_code;
        END;
    ELSE
        v_failed_records_table := ''No Failed Records'';
        v_log_message := ''No failed records found.'';
    END IF;

    v_observed_value := NULL;

    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 5b. Compute column profile metrics and value counts for error records
    ----------------------------------------------------------------------------------------------------
    v_step := ''COMPUTE_ERROR_METRICS'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Computing column profile metrics for error records'');
    
    IF (v_unexpected > 0 AND v_unsupported_conversion_flag = FALSE) THEN
        BEGIN
            -- Compute column profile metrics from failed records (without min/max)
            v_sql := ''SELECT 
                        COUNT(DISTINCT "'' || v_column_nm || ''") AS unique_cnt,
                        CASE WHEN COUNT(*) > 0 THEN (COUNT(DISTINCT "'' || v_column_nm || ''")::FLOAT / COUNT(*)) * 100 ELSE 0 END AS unique_pct
                      FROM '' || v_from_clause || '' WHERE '' || v_where_clause_condition;
            
            v_result := (EXECUTE IMMEDIATE v_sql);
            LET v_profile_cursor CURSOR FOR v_result;
            FOR profile_rec IN v_profile_cursor DO
                v_col_unique_count := profile_rec.unique_cnt;
                v_col_unique_percent := profile_rec.unique_pct;
                BREAK;
            END FOR;
            
            -- Build column profile VARIANT (without min/max values)
            v_column_profile := OBJECT_CONSTRUCT(
                ''column_name'', v_column_nm,
                ''error_total_count'', v_unexpected,
                ''error_unique_percent'', v_col_unique_percent,
                ''error_unique_count'', v_col_unique_count,
                ''missing_percentage'', v_missing_percent * 100,
                ''missing_count'', v_missing_count
            );
            
            -- Compute value counts (frequency of each distinct value excluding nulls and blanks)
            v_sql := ''SELECT OBJECT_AGG(val, cnt) AS value_counts FROM (
                        SELECT TRIM("'' || v_column_nm || ''"::STRING) AS val, COUNT(*) AS cnt 
                        FROM '' || v_from_clause || '' 
                        WHERE ('' || v_where_clause_condition || '') AND "'' || v_column_nm || ''" IS NOT NULL AND TRIM("'' || v_column_nm || ''"::STRING) <> ''''''''
                        GROUP BY TRIM("'' || v_column_nm || ''"::STRING)
                        ORDER BY cnt DESC
                        LIMIT 1000
                      )'';
            
            v_result := (EXECUTE IMMEDIATE v_sql);
            LET v_vc_cursor CURSOR FOR v_result;
            FOR vc_rec IN v_vc_cursor DO
                v_value_counts := OBJECT_CONSTRUCT(''value_counts_without_nan'', vc_rec.value_counts);
                BREAK;
            END FOR;
            
            v_log_message := ''Column profile and value counts computed for error records'';
        EXCEPTION
            WHEN OTHER THEN
                v_error_message := ''Error computing error metrics: '' || SQLERRM;
                v_column_profile := NULL;
                v_value_counts := NULL;
                v_log_message := ''Warning: Could not compute error metrics - '' || SQLERRM;
        END;
    ELSEIF (v_unsupported_conversion_flag = TRUE) THEN
        -- For unsupported conversions, populate column_profile with error details
        v_column_profile := OBJECT_CONSTRUCT(
            ''column_name'', v_column_nm,
            ''element_count'', v_total,
            ''error_count'', v_unexpected,
            ''missing_count'', v_missing_count,
            ''missing_percent'', v_missing_percent * 100,
            ''message'', ''Unsupported casting from '' || COALESCE(v_actual_column_type, ''UNKNOWN'') || '' to '' || COALESCE(v_type_expected, ''UNKNOWN'')
        );
        v_value_counts := NULL;
        v_log_message := ''Unsupported conversion - column profile populated with error details'';
    ELSE
        v_column_profile := NULL;
        v_value_counts := NULL;
        v_log_message := ''No error records - skipping metrics computation'';
    END IF;
    
    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 6. Insert Results into the DQ_RULE_RESULTS Table
    ----------------------------------------------------------------------------------------------------

    v_step := ''INSERT_DQ_RESULTS_TABLE'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Loading results'');

    IF (v_error_message IS NULL) THEN
        BEGIN
        -- Prepare JSON strings
        LET details_json_str STRING := ''{"failed_records_table": "'' || COALESCE(v_failed_records_table, ''null'') || ''", "expected_type": "'' || COALESCE(v_type_expected, ''null'') || ''", "mostly": '' || COALESCE(v_allowed_deviation::STRING, ''null'') || '', "pk_columns_used": "'' || COALESCE(v_key_column_names, ''N/A'') || ''"}'';
        LET results_json_str STRING := ''{'' ||
            ''"element_count": '' || COALESCE(v_total::STRING, ''null'') || '','' ||
            ''"unexpected_count": '' || COALESCE(v_unexpected::STRING, ''null'') || '','' ||
            ''"unexpected_percent": '' || COALESCE(v_percent*100::STRING, ''null'') || '','' ||
            ''"failed_records_table": "'' || COALESCE(v_failed_records_table, ''null'') || ''",'' ||
            ''"observed_value": "'' || COALESCE(v_unexpected::STRING, ''null'') || '' out of '' || COALESCE(v_total::STRING, ''null'') || '' values were not castable to '' || v_type_expected || ''"'' ||
        ''}'';

        -- Use intermediate escaped strings to construct the final SQL reliably
        LET v_rule_str_for_sql STRING := REPLACE(COALESCE(v_input_rule_str, ''null''), '''''''', '''''''''''');
        LET v_results_json_for_sql STRING := REPLACE(results_json_str, '''''''', '''''''''''');
        LET v_details_json_for_sql STRING := REPLACE(details_json_str, '''''''', '''''''''''');
        
        v_sql := ''INSERT INTO "'' || v_dq_db_name || ''"."'' || v_dq_schema_name || ''".DQ_RULE_RESULTS (
            BATCH_ID, DATASET_RUN_ID, DATASET_ID, RULE_CONFIG_ID, EXPECTATION_ID, RUN_NAME, RUN_TIMESTAMP, DATASET_NAME,
            EXPECATION_CONFIG, IS_SUCCESS, RESULTS, EXPECTATION_NAME, DETAILS, ELEMENT_COUNT,
            MISSING_COUNT, MISSING_PERCENT, OBSERVED_VALUE, PARTIAL_UNEXPECTED_COUNTS, PARTIAL_UNEXPECTED_INDEX_LIST, PARTIAL_UNEXPECTED_LIST,
            UNEXPECTED_COUNT, UNEXPECTED_PERCENT, UNEXPECTED_PERCENT_NONMISSING, UNEXPECTED_PERCENT_TOTAL, UNEXPECTED_ROWS, DATA_ROWS, DIMENSION, FAILED_ROWS
            )
            SELECT
            '' || COALESCE(v_batch_id::STRING, ''null'') || '', '' ||
            COALESCE(v_run_id::STRING, ''null'') || '', '' ||
            COALESCE(v_data_asset_id::STRING, ''null'') || '', '' ||
            COALESCE(v_check_config_id::STRING, ''null'') || '', '' ||
            COALESCE(v_expectation_id::STRING, ''null'') || '', '''''' || REPLACE(COALESCE(v_run_name, ''null''), '''''''', '''''''''''') || '''''', CURRENT_TIMESTAMP(), '''''' || REPLACE(COALESCE(v_data_asset_name, ''null''), '''''''', '''''''''''') || '''''', 
            PARSE_JSON('''''' || v_rule_str_for_sql || ''''''), '' || CASE WHEN v_status_code = v_success_code THEN ''TRUE'' ELSE ''FALSE'' END || '', 
            PARSE_JSON('''''' || v_results_json_for_sql || ''''''), '''''' || REPLACE(COALESCE(v_expectation_name, ''null''), '''''''', '''''''''''') || '''''', 
            PARSE_JSON('''''' || v_details_json_for_sql || ''''''), '' ||
            COALESCE(v_total::STRING, ''null'') || '', '' ||
            COALESCE(v_missing_count::STRING, ''null'') || '', '' ||
            COALESCE(v_missing_percent*100::STRING, ''null'') || '', '' ||
            ''NULL::VARIANT, '' ||
            ''PARSE_JSON(\\'''' || REPLACE(COALESCE(v_value_counts::STRING, ''null''), ''\\'''', '''''''''''') || ''\\''), NULL::VARIANT, NULL::VARIANT, '' ||
            COALESCE(v_unexpected::STRING, ''null'') || '', '' ||
            COALESCE(v_percent*100::STRING, ''null'') || '', '' ||
            COALESCE(v_unexpected_percent_nonmissing*100::STRING, ''null'') || '', '' ||
            COALESCE(v_unexpected_percent_total*100::STRING, ''null'') || '', PARSE_JSON(\\'''' || REPLACE(COALESCE(v_column_profile::STRING, ''null''), ''\\'''', '''''''''''') || ''\\''), NULL::VARIANT, '''''' || COALESCE(RULE:DIMENSION, ''null'') || '''''', NULL::VARIANT'';

            EXECUTE IMMEDIATE v_sql;
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
            VALUES (COALESCE(:v_run_id, -1), COALESCE(:v_check_config_id, -1), COALESCE(:v_procedure_name, ''SP_DATA_TYPE_CHECK''), COALESCE(:v_step, ''UNKNOWN''), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''FAILED'', :v_error_message);

        EXCEPTION WHEN OTHER THEN NULL;
        END;
        RETURN COALESCE(v_execution_error, 400);
END;
';
