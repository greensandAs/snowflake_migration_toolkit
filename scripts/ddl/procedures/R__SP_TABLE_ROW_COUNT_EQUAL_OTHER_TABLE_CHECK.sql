USE DATABASE {{ snowflake_database }};

USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE PROCEDURE "SP_TABLE_ROW_COUNT_EQUAL_OTHER_TABLE_CHECK"("RULE" VARIANT)
RETURNS NUMBER(38, 0)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    -- Standard framework variables
    v_sql TEXT;
    v_result RESULTSET;
    v_total INT DEFAULT 0;
    v_status_code_flag INT DEFAULT 0;
    v_status_code NUMBER;
    v_error_message STRING;
    v_step STRING DEFAULT ''INITIALIZATION'';
    v_run_id NUMBER DEFAULT -1;
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
    v_kwargs_variant VARIANT;
    v_batch_id NUMBER DEFAULT -1;
    v_procedure_name STRING;
    v_input_rule_str STRING;
    v_log_message STRING;

    -- Source Table Variables
    v_database_name STRING;
    v_schema_name STRING;
    v_table_name STRING;
    v_dataset_type STRING;
    v_sql_query STRING;
    v_from_clause STRING;
    v_observed_row_count NUMBER DEFAULT 0;

    -- Comparison Dataset Variables (Fetched from DQ_DATASET)
    v_other_dataset_name STRING;
    v_other_type STRING;
    v_other_db STRING;
    v_other_schema STRING;
    v_other_table STRING;
    v_other_sql STRING;
    v_other_from_clause STRING;
    v_expected_row_count NUMBER DEFAULT 0;

    -- Results variables
    v_observed_value VARIANT;

BEGIN
    v_input_rule_str := TO_VARCHAR(RULE);

    ----------------------------------------------------------------------------------------------------
    -- 1. Load configuration
    ----------------------------------------------------------------------------------------------------
    v_step := ''CONFIG_LOADING'';
    v_procedure_name := COALESCE(RULE:PROCEDURE_NAME::STRING, ''SP_TABLE_ROW_COUNT_EQUAL_OTHER_TABLE_CHECK'');
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
            v_error_message := ''Required Configuration parameter is missing in DQ_JOB_EXEC_CONFIG'';
            v_status_code := 400;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_status_code;
        END IF;
    EXCEPTION
        WHEN OTHER THEN
            v_error_message := ''Error loading configuration: '' || SQLERRM;
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN 400;
    END;

    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Configuration loaded successfully'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 2. Parse Source and Fetch Comparison Metadata (DQ_DATASET Lookup)
    ----------------------------------------------------------------------------------------------------
    v_step := ''RULE_PARSING'';

    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Parsing rule and fetching comparison metadata'');
    
    -- Parse Source Assets
    v_batch_id := COALESCE(RULE:BATCH_ID::NUMBER, -1);
    v_data_asset_id := COALESCE(RULE:DATASET_ID::NUMBER, -1);
    v_expectation_id := COALESCE(RULE:EXPECTATION_ID::NUMBER, -1);
    v_database_name := RULE:DATABASE_NAME::STRING;
    v_schema_name := RULE:SCHEMA_NAME::STRING;
    v_table_name := RULE:TABLE_NAME::STRING;
    v_run_name := RULE:RUN_NAME::STRING;
    v_data_asset_name := RULE:DATASET_NAME::STRING;
    v_expectation_name := RULE:EXPECTATION_NAME::STRING;
    v_dataset_type := UPPER(COALESCE(RULE:DATASET_TYPE::STRING, ''TABLE''));
    v_sql_query := RULE:CUSTOM_SQL::STRING;
    v_kwargs_variant := PARSE_JSON(RULE:KWARGS);
    
    -- Identify the Comparison Dataset Name from KWARGS
    v_other_dataset_name := v_kwargs_variant:other_dataset_name::STRING;

    IF (v_other_dataset_name IS NULL) THEN
        v_error_message := ''KWARGS must contain other_dataset_name'';
        UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
        RETURN v_execution_error;
    END IF;

    -- FETCH Comparison details from DQ_DATASET table
    BEGIN
        v_sql := ''SELECT DATASET_TYPE, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, CUSTOM_SQL 
                  FROM "'' || v_dq_db_name || ''"."'' || v_dq_schema_name || ''".DQ_DATASET 
                  WHERE DATASET_NAME = '''''' || v_other_dataset_name || '''''' LIMIT 1'';
        
        v_result := (EXECUTE IMMEDIATE v_sql);
        LET v_ds_cursor CURSOR FOR v_result;
        FOR ds_record IN v_ds_cursor DO
            v_other_type := UPPER(ds_record.DATASET_TYPE);
            v_other_db := ds_record.DATABASE_NAME;
            v_other_schema := ds_record.SCHEMA_NAME;
            v_other_table := ds_record.TABLE_NAME;
            v_other_sql := ds_record.CUSTOM_SQL;
        END FOR;

        IF (v_other_type IS NULL) THEN
            v_error_message := ''Comparison dataset '''''' || v_other_dataset_name || '''''' not found in DQ_DATASET.'';
            UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_execution_error;
        END IF;
    EXCEPTION WHEN OTHER THEN
        v_error_message := ''Error fetching comparison metadata: '' || SQLERRM;
        UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
        RETURN v_execution_error;
    END;

    -- Build Source FROM clause
    IF (v_dataset_type = ''QUERY'') THEN
        v_from_clause := ''('' || v_sql_query || '') AS src_query'';
    ELSE
        v_from_clause := ''"'' || v_database_name || ''"."'' || v_schema_name || ''"."'' || v_table_name || ''"'';
    END IF;

    -- Build Comparison FROM clause
    IF (v_other_type = ''QUERY'') THEN
        v_other_from_clause := ''('' || v_other_sql || '') AS comp_query'';
    ELSE
        v_other_from_clause := ''"'' || v_other_db || ''"."'' || v_other_schema || ''"."'' || v_other_table || ''"'';
    END IF;

    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Rule parsed and comparison metadata fetched successfully'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 3. Execute Row Count Queries
    ----------------------------------------------------------------------------------------------------
    v_step := ''MAIN_QUERY'';

    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Executing row count queries'');

    -- Count Source
    BEGIN
        v_sql := ''SELECT COUNT(*) AS row_cnt FROM '' || v_from_clause;
        v_result := (EXECUTE IMMEDIATE v_sql);
        LET v_src_c CURSOR FOR v_result;
        FOR r IN v_src_c DO v_observed_row_count := r.row_cnt; END FOR;
        v_observed_value := v_observed_row_count;
        v_total := v_observed_row_count;
    EXCEPTION WHEN OTHER THEN
        v_error_message := ''Error counting source: '' || SQLERRM;
        UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
        RETURN v_execution_error;
    END;

    -- Count Comparison
    BEGIN
        v_sql := ''SELECT COUNT(*) AS row_cnt FROM '' || v_other_from_clause;
        v_result := (EXECUTE IMMEDIATE v_sql);
        LET v_comp_c CURSOR FOR v_result;
        FOR r IN v_comp_c DO v_expected_row_count := r.row_cnt; END FOR;
    EXCEPTION WHEN OTHER THEN
        v_error_message := ''Error counting comparison: '' || SQLERRM;
        UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
        RETURN v_execution_error;
    END;

    -- Compare
    IF (v_observed_row_count != v_expected_row_count) THEN
        v_status_code_flag := 1;
        v_log_message := ''FAILED: Source ('' || v_observed_row_count || '') != '' || v_other_dataset_name || '' ('' || v_expected_row_count || '').'';
    ELSE
        v_log_message := ''PASSED: Counts match ('' || v_observed_row_count || '').'';
    END IF;

    v_status_code := CASE WHEN v_status_code_flag = 0 THEN v_success_code ELSE v_failed_code END;

    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    ----------------------------------------------------------------------------------------------------
    -- 4. Log Results into DQ_RULE_RESULTS
    ----------------------------------------------------------------------------------------------------
    v_step := ''INSERT_DQ_RESULTS_TABLE'';

    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, LOG_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'', ''Inserting results into DQ_RULE_RESULTS'');
    
    LET details_json_str STRING := ''{'' ||
         ''"comparison_dataset_name": "'' || v_other_dataset_name || ''",'' ||
         ''"expected_value": '' || v_expected_row_count::STRING ||
    ''}'';

    LET results_json_str STRING := ''{'' ||
        ''"observed_value": '' || v_observed_row_count::STRING || '','' ||
        ''"element_count": '' || v_total::STRING || '','' ||
        ''"expected_value": '' || v_expected_row_count::STRING || '','' ||
        ''"success": '' || CASE WHEN v_status_code = v_success_code THEN ''true'' ELSE ''false'' END || 
    ''}'';

    v_sql := ''INSERT INTO "'' || v_dq_db_name || ''"."'' || v_dq_schema_name || ''".DQ_RULE_RESULTS (
                BATCH_ID, DATASET_RUN_ID, DATASET_ID, RULE_CONFIG_ID, EXPECTATION_ID, RUN_NAME, RUN_TIMESTAMP, DATASET_NAME,
                EXPECATION_CONFIG, IS_SUCCESS, RESULTS, EXPECTATION_NAME, DETAILS, ELEMENT_COUNT, MISSING_COUNT,
                OBSERVED_VALUE, UNEXPECTED_COUNT, "USER", SESSION_ID
                )
                SELECT 
                '' || COALESCE(v_batch_id::STRING, ''null'') || '', '' ||
                COALESCE(v_run_id::STRING, ''null'') || '', '' ||
                COALESCE(v_data_asset_id::STRING, ''null'') || '', '' ||
                COALESCE(v_check_config_id::STRING, ''null'') || '', '' ||
                COALESCE(v_expectation_id::STRING, ''null'') || '', '''''' || REPLACE(v_run_name, '''''''', '''''''''''') || '''''', CURRENT_TIMESTAMP(), '''''' || REPLACE(v_data_asset_name, '''''''', '''''''''''') || '''''', 
                PARSE_JSON('''''' || REPLACE(v_input_rule_str, '''''''', '''''''''''') || ''''''), 
                '' || CASE WHEN v_status_code = v_success_code THEN ''TRUE'' ELSE ''FALSE'' END || '', 
                PARSE_JSON('''''' || REPLACE(results_json_str, '''''''', '''''''''''') || ''''''), 
                '''''' || REPLACE(v_expectation_name, '''''''', '''''''''''') || '''''', 
                PARSE_JSON('''''' || REPLACE(details_json_str, '''''''', '''''''''''') || ''''''), 
                '' || v_total || '', 0, '' || v_observed_row_count || '', 0, CURRENT_USER(), CURRENT_SESSION()'';

    EXECUTE IMMEDIATE v_sql;

    UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''COMPLETED'', LOG_MESSAGE = ''Results inserted successfully'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    RETURN v_status_code;

EXCEPTION
    WHEN OTHER THEN
        v_error_message := ''Global exception at step '' || v_step || '': '' || SQLERRM;
        UPDATE DQ_RULE_AUDIT_LOG SET END_TIMESTAMP = CURRENT_TIMESTAMP(), STATUS = ''FAILED'', ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
        RETURN COALESCE(v_execution_error, 400);
END;
';
