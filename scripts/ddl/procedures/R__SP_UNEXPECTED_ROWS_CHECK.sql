USE DATABASE {{ snowflake_database }};

USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE PROCEDURE "SP_UNEXPECTED_ROWS_CHECK"("RULE" VARIANT)
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
    v_allowed_deviation FLOAT DEFAULT 0;
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
    v_log_message STRING;
    v_unexpected_rows_query STRING; 
    v_final_unexpected_query STRING;
    v_physical_source STRING;
    v_dimension STRING;
    v_clean_dataset_name STRING;
    v_full_target_table_name STRING;
    v_failed_records_table STRING DEFAULT NULL;
    v_status_code_flag INT DEFAULT 0;

BEGIN
    v_procedure_name := COALESCE(RULE:PROCEDURE_NAME::STRING, ''SP_UNEXPECTED_ROWS_CHECK'');
    v_run_id := COALESCE(RULE:DATASET_RUN_ID::NUMBER, -1);
    v_check_config_id := COALESCE(RULE:RULE_CONFIG_ID::NUMBER, -1);

    v_step := ''PROCEDURE_START'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'');
    UPDATE DQ_RULE_AUDIT_LOG SET STATUS = ''COMPLETED'', END_TIMESTAMP = CURRENT_TIMESTAMP(), LOG_MESSAGE = ''Procedure execution started'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    v_step := ''CONFIG_LOADING'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'');

    BEGIN
        SELECT DQ_DB_NAME, DQ_SCHEMA_NAME, SUCCESS_CODE, FAILED_CODE, EXECUTION_ERROR 
        INTO :v_dq_db_name, :v_dq_schema_name, :v_success_code, :v_failed_code, :v_execution_error
        FROM DQ_JOB_EXEC_CONFIG LIMIT 1;
        
        IF (v_dq_db_name IS NULL) THEN
            UPDATE DQ_RULE_AUDIT_LOG SET STATUS = ''FAILED'', ERROR_MESSAGE = ''Missing config'' WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN 400;
        END IF;
        v_log_message := ''Configuration captured: DB='' || v_dq_db_name || '', Schema='' || v_dq_schema_name || '', Success='' || v_success_code || '', Failed='' || v_failed_code;
        UPDATE DQ_RULE_AUDIT_LOG SET STATUS = ''COMPLETED'', END_TIMESTAMP = CURRENT_TIMESTAMP(), LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
    EXCEPTION WHEN OTHER THEN
        v_error_message := SQLERRM;
        UPDATE DQ_RULE_AUDIT_LOG SET STATUS = ''FAILED'', END_TIMESTAMP = CURRENT_TIMESTAMP(), ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
        RETURN 400;
    END;

    -- STEP 1: Extract rule variables from JSON
    v_step := ''RULE_PARSING_EXTRACT_VARS'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'');

    BEGIN
        v_batch_id := COALESCE(RULE:BATCH_ID::NUMBER, -1);
        v_data_asset_id := COALESCE(RULE:DATASET_ID::NUMBER, -1);
        v_expectation_id := COALESCE(RULE:EXPECTATION_ID::NUMBER, -1);
        v_data_asset_name := RULE:DATASET_NAME::STRING;
        v_expectation_name := RULE:EXPECTATION_NAME::STRING;
        v_kwargs_variant := PARSE_JSON(RULE:KWARGS);
        v_allowed_deviation := COALESCE(v_kwargs_variant:mostly::FLOAT, 1.0);
        v_unexpected_rows_query := v_kwargs_variant:unexpected_rows_query::STRING; 
        v_dimension := RULE:DIMENSION::STRING;

        v_log_message := ''Variables extracted: Dataset='' || v_data_asset_name || '', Expectation='' || v_expectation_name || '', Deviation='' || v_allowed_deviation;
        UPDATE DQ_RULE_AUDIT_LOG SET STATUS = ''COMPLETED'', END_TIMESTAMP = CURRENT_TIMESTAMP(), LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
    EXCEPTION WHEN OTHER THEN
        v_error_message := SQLERRM;
        UPDATE DQ_RULE_AUDIT_LOG SET STATUS = ''FAILED'', END_TIMESTAMP = CURRENT_TIMESTAMP(), ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
        RETURN v_execution_error;
    END;

    -- STEP 2: Build dataset mapping query
    v_step := ''RULE_PARSING_BUILD_MAPPING'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'');

    LET v_mapping_array VARIANT;
    LET v_match_count INT DEFAULT 0;
    LET v_first_dataset_name STRING;

    BEGIN
        v_first_dataset_name := UPPER(REGEXP_SUBSTR(v_unexpected_rows_query, ''FROM\\\\s+([A-Za-z0-9_]+)'', 1, 1, ''ie'', 1));
        
        LET v_escaped_query STRING;
        v_escaped_query := REPLACE(REPLACE(v_unexpected_rows_query, ''\\\\'', ''\\\\\\\\''), '''''''', '''''''''''');
        
        v_sql := ''WITH tokens AS (
            SELECT DISTINCT value::STRING AS word
            FROM TABLE(FLATTEN(input => REGEXP_SUBSTR_ALL('''''' || v_escaped_query || '''''', ''''\\\\\\\\b\\\\\\\\w+\\\\\\\\b'''')))
        ),
        matches AS (
            SELECT t.word AS original_name, 
                   CASE WHEN UPPER(d.DATASET_TYPE) = ''''QUERY'''' 
                        THEN ''''('''' || d.CUSTOM_SQL || '''')''''
                        ELSE ''''"'''' || d.DATABASE_NAME || ''''"."'''' || d.SCHEMA_NAME || ''''"."'''' || d.TABLE_NAME || ''''"''''
                   END AS target_name
            FROM tokens t
            JOIN "'' || v_dq_db_name || ''"."'' || v_dq_schema_name || ''".DQ_DATASET d 
            ON UPPER(t.word) = UPPER(d.DATASET_NAME)
        )
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(''''old'''', original_name, ''''new'''', target_name)) AS mapping_array,
               COUNT(*) AS match_count
        FROM matches'';

        v_log_message := ''Mapping query built. First dataset='' || v_first_dataset_name;
        UPDATE DQ_RULE_AUDIT_LOG SET STATUS = ''COMPLETED'', END_TIMESTAMP = CURRENT_TIMESTAMP(), LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
    EXCEPTION WHEN OTHER THEN
        v_error_message := SQLERRM;
        UPDATE DQ_RULE_AUDIT_LOG SET STATUS = ''FAILED'', END_TIMESTAMP = CURRENT_TIMESTAMP(), ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
        RETURN v_execution_error;
    END;

    -- STEP 3: Execute mapping query
    v_step := ''RULE_PARSING_EXEC_MAPPING'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'');

    BEGIN
        v_result := (EXECUTE IMMEDIATE v_sql);
        LET c1 CURSOR FOR v_result;
        FOR rec IN c1 DO
            v_mapping_array := rec.MAPPING_ARRAY;
            v_match_count := rec.MATCH_COUNT;
        END FOR;

        v_log_message := ''Mapping executed. Match count='' || v_match_count;
        UPDATE DQ_RULE_AUDIT_LOG SET STATUS = ''COMPLETED'', END_TIMESTAMP = CURRENT_TIMESTAMP(), LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
    EXCEPTION WHEN OTHER THEN
        v_error_message := SQLERRM;
        UPDATE DQ_RULE_AUDIT_LOG SET STATUS = ''FAILED'', END_TIMESTAMP = CURRENT_TIMESTAMP(), ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
        RETURN v_execution_error;
    END;

    -- STEP 4: Apply dataset replacements
    v_step := ''RULE_PARSING_APPLY_REPLACE'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'');

    BEGIN
        v_final_unexpected_query := v_unexpected_rows_query;
        
        IF (v_match_count = 0 OR v_mapping_array IS NULL) THEN
            v_physical_source := REGEXP_SUBSTR(v_unexpected_rows_query, ''FROM\\\\s+([A-Za-z0-9_]+(\\\\.[A-Za-z0-9_]+)*)'', 1, 1, ''ie'', 1);
        ELSE
            FOR i IN 0 TO ARRAY_SIZE(v_mapping_array) - 1 DO
                v_final_unexpected_query := REGEXP_REPLACE(
                    v_final_unexpected_query,
                    ''\\\\b'' || v_mapping_array[i]:old::STRING || ''\\\\b'',
                    v_mapping_array[i]:new::STRING,
                    1, 0, ''i''
                );
                IF (UPPER(v_mapping_array[i]:old::STRING) = v_first_dataset_name) THEN
                    v_physical_source := v_mapping_array[i]:new::STRING;
                END IF;
            END FOR;

            IF (v_physical_source IS NULL) THEN
                v_physical_source := v_mapping_array[0]:new::STRING;
            END IF;
        END IF;
        
        v_log_message := ''Replacements applied. Datasets replaced='' || v_match_count || '', Primary source='' || v_physical_source;
        UPDATE DQ_RULE_AUDIT_LOG SET STATUS = ''COMPLETED'', END_TIMESTAMP = CURRENT_TIMESTAMP(), LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
    EXCEPTION WHEN OTHER THEN
        v_error_message := SQLERRM;
        UPDATE DQ_RULE_AUDIT_LOG SET STATUS = ''FAILED'', END_TIMESTAMP = CURRENT_TIMESTAMP(), ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
        RETURN v_execution_error;
    END;

    v_step := ''MAIN_QUERY'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'');

    BEGIN
        v_sql := ''SELECT COUNT(*) AS CNT FROM '' || v_physical_source;
        v_result := (EXECUTE IMMEDIATE v_sql);
        LET c2 CURSOR FOR v_result;
        FOR rec IN c2 DO
            v_total := rec.CNT;
        END FOR;

        v_sql := ''SELECT COUNT(*) AS CNT FROM ('' || v_final_unexpected_query || '')'';
        v_result := (EXECUTE IMMEDIATE v_sql);
        LET c3 CURSOR FOR v_result;
        FOR rec IN c3 DO
            v_unexpected := rec.CNT;
        END FOR;
        
        v_percent := CASE WHEN v_total = 0 THEN 0 ELSE (v_unexpected::FLOAT / v_total) END;
        v_status_code_flag := CASE WHEN v_unexpected > 0 THEN 1 ELSE 0 END;
        v_status_code := CASE WHEN v_status_code_flag = 0 THEN v_success_code ELSE v_failed_code END;
        v_log_message := CASE WHEN v_status_code_flag = 0 THEN ''CHECK PASSED'' ELSE ''CHECK FAILED'' END || '': Total='' || v_total || '', Unexpected='' || v_unexpected || '', Percent='' || ROUND(v_percent * 100, 2) || ''%'';
        UPDATE DQ_RULE_AUDIT_LOG SET STATUS = ''COMPLETED'', END_TIMESTAMP = CURRENT_TIMESTAMP(), LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
    EXCEPTION WHEN OTHER THEN
        v_error_message := SQLERRM;
        UPDATE DQ_RULE_AUDIT_LOG SET STATUS = ''FAILED'', END_TIMESTAMP = CURRENT_TIMESTAMP(), ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
        RETURN v_execution_error;
    END;

    v_step := ''CAPTURE_FAILURES'';
    IF (v_unexpected > 0) THEN
        INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS)
        VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'');
        BEGIN
            v_clean_dataset_name := REGEXP_REPLACE(v_data_asset_name, ''[^a-zA-Z0-9]'', ''_'');
            v_failed_records_table := v_clean_dataset_name || ''_DQ_FAILURE'';
            v_full_target_table_name := ''"'' || v_dq_db_name || ''"."'' || v_dq_schema_name || ''"."'' || v_failed_records_table || ''"'';
            EXECUTE IMMEDIATE ''CREATE TABLE IF NOT EXISTS '' || v_full_target_table_name || '' AS SELECT '' || v_run_id || ''::NUMBER(38,0) AS DATASET_RUN_ID, '' || v_data_asset_id || ''::NUMBER(38,0) AS DATASET_ID, '' || v_check_config_id || ''::NUMBER(38,0) AS RULE_CONFIG_ID, CURRENT_TIMESTAMP()::TIMESTAMP_LTZ AS DQ_LOAD_TIMESTAMP, * FROM ('' || v_final_unexpected_query || '') WHERE 1=0'';
            
            EXECUTE IMMEDIATE ''INSERT INTO '' || v_full_target_table_name || '' SELECT '' || v_run_id || '', '' || v_data_asset_id || '', '' || v_check_config_id || '', CURRENT_TIMESTAMP(), * FROM ('' || v_final_unexpected_query || '')'';
            v_log_message := ''Captured '' || v_unexpected || '' failed records into '' || v_failed_records_table;
            UPDATE DQ_RULE_AUDIT_LOG SET STATUS = ''COMPLETED'', END_TIMESTAMP = CURRENT_TIMESTAMP(), LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
        EXCEPTION WHEN OTHER THEN
            v_error_message := SQLERRM;
            UPDATE DQ_RULE_AUDIT_LOG SET STATUS = ''FAILED'', END_TIMESTAMP = CURRENT_TIMESTAMP(), ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
            RETURN v_execution_error;
        END;
    END IF;

    v_step := ''INSERT_DQ_RESULTS_TABLE'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'');
    BEGIN
        LET results_json VARIANT := OBJECT_CONSTRUCT(''total'', v_total, ''unexpected'', v_unexpected, ''percent'', v_percent * 100, ''failed_records_table'', v_failed_records_table);
        LET v_is_success_str STRING := CASE WHEN v_status_code_flag = 0 THEN ''TRUE'' ELSE ''FALSE'' END;
        LET v_run_name STRING := ''DQ_RUN_'' || v_run_id;
        LET v_kwargs_variant VARIANT := COALESCE(TRY_PARSE_JSON(RULE:KWARGS::STRING), OBJECT_CONSTRUCT());
        
        INSERT INTO DQ_RULE_RESULTS (
            BATCH_ID, DATASET_RUN_ID, DATASET_ID, RULE_CONFIG_ID, EXPECTATION_ID, 
            RUN_NAME, DATASET_NAME, EXPECTATION_NAME, EXPECATION_CONFIG, DETAILS,
            ELEMENT_COUNT, MISSING_COUNT, UNEXPECTED_COUNT, UNEXPECTED_PERCENT, IS_SUCCESS, RESULTS, DIMENSION, RUN_TIMESTAMP
        ) SELECT 
            :v_batch_id, 
            :v_run_id, 
            :v_data_asset_id, 
            :v_check_config_id, 
            :v_expectation_id, 
            :v_run_name, 
            COALESCE(:v_data_asset_name, ''UNKNOWN''), 
            COALESCE(:v_expectation_name, ''UnexpectedRowsCheck''),
            TO_VARIANT(:v_unexpected_rows_query),
            :v_kwargs_variant,
            :v_total, 
            0, 
            :v_unexpected, 
            :v_percent * 100, 
            CASE WHEN :v_status_code_flag = 0 THEN TRUE ELSE FALSE END, 
            :results_json, 
            :v_dimension, 
            CURRENT_TIMESTAMP();
            
        v_log_message := ''Results loaded to DQ_RULE_RESULTS: Success='' || CASE WHEN v_status_code_flag = 0 THEN ''TRUE'' ELSE ''FALSE'' END;
        UPDATE DQ_RULE_AUDIT_LOG SET STATUS = ''COMPLETED'', END_TIMESTAMP = CURRENT_TIMESTAMP(), LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
    EXCEPTION WHEN OTHER THEN
        v_error_message := SQLERRM;
        UPDATE DQ_RULE_AUDIT_LOG SET STATUS = ''FAILED'', END_TIMESTAMP = CURRENT_TIMESTAMP(), ERROR_MESSAGE = :v_error_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;
        RETURN v_execution_error;
    END;

    v_step := ''PROCEDURE_COMPLETE'';
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, :v_step, CURRENT_TIMESTAMP(), ''STARTED'');
    v_log_message := ''Procedure completed successfully with status_code='' || v_status_code;
    UPDATE DQ_RULE_AUDIT_LOG SET STATUS = ''COMPLETED'', END_TIMESTAMP = CURRENT_TIMESTAMP(), LOG_MESSAGE = :v_log_message WHERE DATASET_RUN_ID = :v_run_id AND RULE_CONFIG_ID = :v_check_config_id AND STEP_NAME = :v_step;

    RETURN v_status_code;
EXCEPTION WHEN OTHER THEN
    v_error_message := SQLERRM;
    INSERT INTO DQ_RULE_AUDIT_LOG (DATASET_RUN_ID, RULE_CONFIG_ID, PROCEDURE_NAME, STEP_NAME, START_TIMESTAMP, STATUS, ERROR_MESSAGE)
    VALUES (:v_run_id, :v_check_config_id, :v_procedure_name, ''GLOBAL_EXCEPTION'', CURRENT_TIMESTAMP(), ''FAILED'', :v_error_message);
    RETURN 400;
END;
';
