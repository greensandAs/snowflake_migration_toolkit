USE DATABASE {{ snowflake_database }};

USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE PROCEDURE "SP_DATA_PROFILER_NATIVE"(
    "RUN_ID" VARCHAR, "DATASET_ID" VARCHAR, "DATASET_NAME" VARCHAR, "DBNAME" VARCHAR, "SCHEMANAME" VARCHAR, "TABLENAME" VARCHAR, "CUSTOMSQL" VARCHAR
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run_profiler_native'
EXECUTE AS CALLER
AS '
import json
import re
import traceback
import uuid
from datetime import datetime
import snowflake.snowpark.functions as F
from snowflake.snowpark.functions import call_function
from snowflake.snowpark.types import StringType, IntegerType, DoubleType, DecimalType, LongType, DateType, TimestampType, FloatType

WH_SIZE_CONFIG = {
    "X-Small":  (15, 4),
    "Small":    (20, 6),
    "Medium":   (25, 10),
    "Large":    (30, 16),
    "X-Large":  (40, 24),
    "2X-Large": (40, 24),
    "3X-Large": (40, 24),
    "4X-Large": (40, 24),
    "5X-Large": (40, 24),
    "6X-Large": (40, 24),
}
DEFAULT_BATCH_SIZE = 20
DEFAULT_MAX_ASYNC = 6

def _get_warehouse_config(session):
    try:
        wh_name = session.get_current_warehouse()
        if not wh_name:
            return DEFAULT_BATCH_SIZE, DEFAULT_MAX_ASYNC
        wh_name_clean = wh_name.strip(''"'')
        rows = session.sql(f"SHOW WAREHOUSES LIKE ''{wh_name_clean}''").collect()
        if rows:
            size = rows[0]["size"]
            return WH_SIZE_CONFIG.get(size, (DEFAULT_BATCH_SIZE, DEFAULT_MAX_ASYNC))
    except Exception:
        pass
    return DEFAULT_BATCH_SIZE, DEFAULT_MAX_ASYNC

CUSTOM_SQL_BLOCKED_KEYWORDS = [
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE",
    "CREATE", "REPLACE", "MERGE", "GRANT", "REVOKE", "CALL",
    "EXECUTE", "PUT", "GET", "COPY", "REMOVE", "UNDROP",
]

def _strip_sql_comments(sql_text: str) -> str:
    no_block = re.sub(r"/*.*?*/", " ", sql_text, flags=re.DOTALL)
    no_line = re.sub(r"--[^\\n]*", " ", no_block)
    return no_line

def validate_custom_sql(sql_text: str) -> None:
    if not sql_text or not sql_text.strip():
        raise ValueError("Custom SQL cannot be empty")
    stripped = _strip_sql_comments(sql_text)
    normalized = stripped.strip().upper()
    if not normalized.startswith("SELECT") and not normalized.startswith("WITH"):
        raise ValueError("Custom SQL must be a SELECT or WITH query")
    tokens = re.findall(r"[A-Z_]+", normalized)
    for kw in CUSTOM_SQL_BLOCKED_KEYWORDS:
        if kw in tokens:
            raise ValueError(f"Custom SQL contains disallowed keyword: {kw}")

def q_ident(name: str) -> str:
    if not name or name.strip() == '''': return ''NULL'' 
    name = name.strip(''"'') 
    return ''"'' + name.replace(''"'', ''""'') + ''"''

def q_str(val: str) -> str:
    if val is None: return ''NULL''
    if str(val).strip() == '''': return ''NULL''
    return "''" + str(val).replace("''", "''''") + "''"

TYPE_BYTE_ESTIMATES = {
    IntegerType: 8, LongType: 8, FloatType: 4, DoubleType: 8, DecimalType: 16,
    DateType: 4, TimestampType: 8,
}

def _estimate_col_bytes(col_type, non_null, avg_len):
    if isinstance(col_type, StringType):
        return int((avg_len or 0) * non_null)
    for t, b in TYPE_BYTE_ESTIMATES.items():
        if isinstance(col_type, t):
            return b * non_null
    return 8 * non_null

def _fetch_column_types(session, db, schema, table):
    sql = (
        f"SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, "
        f"CHARACTER_MAXIMUM_LENGTH, DATETIME_PRECISION "
        f"FROM {q_ident(db)}.INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA = {q_str(schema)} AND TABLE_NAME = {q_str(table)} "
        f"ORDER BY ORDINAL_POSITION"
    )
    rows = session.sql(sql).collect()
    type_map = {}
    for row in rows:
        col = row["COLUMN_NAME"]
        dt = row["DATA_TYPE"].upper()
        prec = row["NUMERIC_PRECISION"]
        scale = row["NUMERIC_SCALE"]
        char_len = row["CHARACTER_MAXIMUM_LENGTH"]
        dt_prec = row["DATETIME_PRECISION"]
        if dt in ("NUMBER", "DECIMAL", "NUMERIC") and prec is not None:
            s = int(scale) if scale is not None else 0
            if s > 0:
                type_map[col] = f"{dt}({int(prec)},{s})"
            else:
                type_map[col] = f"{dt}({int(prec)},0)"
        elif dt in ("TEXT", "VARCHAR"):
            type_map[col] = "TEXT"
        elif dt in ("TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ") and dt_prec is not None:
            type_map[col] = f"{dt}({int(dt_prec)})"
        else:
            type_map[col] = dt
    return type_map

def build_col_aggs(fields):
    aggs = []
    col_meta = []
    for field in fields:
        col_name = field.name
        col_type = field.datatype
        is_numeric = isinstance(col_type, (IntegerType, DoubleType, DecimalType, LongType, FloatType))
        is_string = isinstance(col_type, StringType)
        prefix = col_name.replace(''"'', ''_'')
        aggs.extend([
            F.count(F.col(col_name)).alias(f"{prefix}__NON_NULL"),
            F.approx_count_distinct(F.col(col_name)).alias(f"{prefix}__DISTINCT"),
            F.min(F.col(col_name)).alias(f"{prefix}__MIN"),
            F.max(F.col(col_name)).alias(f"{prefix}__MAX"),
        ])
        if is_numeric:
            aggs.extend([
                # F.avg(F.col(col_name)).alias(f"{prefix}__MEAN"),
                F.stddev(F.col(col_name)).alias(f"{prefix}__STDDEV"),
                # call_function("APPROX_PERCENTILE", F.col(col_name), F.lit(0.05)).alias(f"{prefix}__P05"),
                call_function("APPROX_PERCENTILE", F.col(col_name), F.lit(0.25)).alias(f"{prefix}__P25"),
                call_function("APPROX_PERCENTILE", F.col(col_name), F.lit(0.50)).alias(f"{prefix}__P50"),
                call_function("APPROX_PERCENTILE", F.col(col_name), F.lit(0.75)).alias(f"{prefix}__P75"),
                call_function("APPROX_PERCENTILE", F.col(col_name), F.lit(0.95)).alias(f"{prefix}__P95"),
                F.sum(F.iff(F.col(col_name) == 0, 1, 0)).alias(f"{prefix}__ZERO"),
                # F.sum(F.col(col_name)).alias(f"{prefix}__SUM"),
                # F.sum(F.iff(F.col(col_name) < 0, 1, 0)).alias(f"{prefix}__NEG"),
                # F.kurtosis(F.col(col_name)).alias(f"{prefix}__KURT"),
                # F.skew(F.col(col_name)).alias(f"{prefix}__SKEW"),
            ])
        if is_string:
            aggs.extend([
                F.avg(F.length(F.col(col_name))).alias(f"{prefix}__AVGLEN"),
                # F.max(F.length(F.col(col_name))).alias(f"{prefix}__MAXLEN"),
                # F.min(F.length(F.col(col_name))).alias(f"{prefix}__MINLEN"),
                # F.median(F.length(F.col(col_name))).alias(f"{prefix}__MEDLEN"),
                F.sum(F.iff(
                    (F.try_cast(F.col(col_name), DateType()).is_null()) & (F.col(col_name).is_not_null()),
                    1, 0
                )).alias(f"{prefix}__INVDATE"),
            ])
        col_meta.append({"name": col_name, "type": col_type, "prefix": prefix, "is_numeric": is_numeric, "is_string": is_string})
    return aggs, col_meta

# --- COMMENTED OUT: Expensive helper functions (uncomment to enable) ---
# def build_value_counts_sql(table_path, fields, sample_pct=50):
#     parts = []
#     for field in fields:
#         col_name = field.name
#         prefix = col_name.replace(''"'', ''_'')
#         q_col = q_ident(col_name)
#         alias = chr(34) + prefix + "__VCOUNTS" + chr(34)
#         parts.append(
#             "(SELECT OBJECT_AGG(" + q_col + "::VARCHAR, cnt::VARIANT) FROM (SELECT " + q_col + ", COUNT(*) AS cnt FROM " + table_path + " TABLESAMPLE (" + str(sample_pct) + ") WHERE " + q_col + " IS NOT NULL GROUP BY " + q_col + " ORDER BY cnt DESC LIMIT 50)) AS " + alias
#         )
#     if not parts:
#         return None
#     return "SELECT " + ", ".join(parts)
#
# def build_length_dist_sql(table_path, fields):
#     string_fields = [f for f in fields if isinstance(f.datatype, StringType)]
#     if not string_fields:
#         return None
#     parts = []
#     for field in string_fields:
#         col_name = field.name
#         prefix = col_name.replace(''"'', ''_'')
#         q_col = q_ident(col_name)
#         alias = chr(34) + prefix + "__LENDIST" + chr(34)
#         parts.append(
#             "(SELECT OBJECT_AGG(len::VARCHAR, cnt::VARIANT) FROM (SELECT LENGTH(" + q_col + ") AS len, COUNT(*) AS cnt FROM " + table_path + " WHERE " + q_col + " IS NOT NULL GROUP BY len ORDER BY len LIMIT 50)) AS " + alias
#         )
#     return "SELECT " + ", ".join(parts)
#
# def build_unique_count_sql(table_path, fields):
#     selects = []
#     for field in fields:
#         col_name = field.name
#         prefix = col_name.replace(''"'', ''_'')
#         q_col = q_ident(col_name)
#         alias = chr(34) + prefix + "__UNIQUE" + chr(34)
#         selects.append(
#             "(SELECT COUNT(*) FROM (SELECT " + q_col + " FROM " + table_path + " WHERE " + q_col + " IS NOT NULL GROUP BY " + q_col + " HAVING COUNT(*) = 1)) AS " + alias
#         )
#     if not selects:
#         return None
#     return "SELECT " + ", ".join(selects)

def parse_batch_result(res, col_meta_list, total_rows, sample_rows, run_id, dataset_id, col_idx_offset=0, col_type_map=None):
    stats = []
    for i, meta in enumerate(col_meta_list):
        prefix = meta["prefix"]
        col_name = meta["name"]
        col_type = meta["type"]
        is_numeric = meta["is_numeric"]
        is_string = meta["is_string"]
        non_null = res[f"{prefix}__NON_NULL"]
        distinct_count = res[f"{prefix}__DISTINCT"]
        missing_count = total_rows - non_null
        missing_pct = (missing_count / total_rows * 100) if total_rows > 0 else 0
        distinct_pct = (distinct_count / total_rows * 100) if total_rows > 0 else 0
        # unique_count = 0
        # if unique_res is not None:
        #     unique_count = unique_res.get(f"{prefix}__UNIQUE", 0) or 0
        # unique_pct = (unique_count / total_rows * 100) if total_rows > 0 else 0
        # is_unique = (unique_count == non_null and non_null > 0)
        zero_count = res.get(f"{prefix}__ZERO", 0) if is_numeric else 0
        # zero_pct = (zero_count / total_rows * 100) if total_rows > 0 else 0
        # neg_count = res.get(f"{prefix}__NEG", 0) if is_numeric else 0
        # neg_pct = (neg_count / total_rows * 100) if total_rows > 0 else 0
        avg_len = res.get(f"{prefix}__AVGLEN") if is_string else None
        # estimated_bytes = _estimate_col_bytes(col_type, non_null, avg_len)
        inv_date = res.get(f"{prefix}__INVDATE", 0) if is_string else 0
        # inv_date_pct = (inv_date / non_null * 100) if (is_string and non_null > 0) else 0
        # vc_str = None
        # if vcounts_res is not None:
        #     raw = vcounts_res.get(f"{prefix}__VCOUNTS")
        #     if raw is not None:
        #         vc_str = raw if isinstance(raw, str) else json.dumps(raw, ensure_ascii=False)
        # ld_str = None
        # if lendist_res is not None and is_string:
        #     raw = lendist_res.get(f"{prefix}__LENDIST")
        #     if raw is not None:
        #         ld_str = raw if isinstance(raw, str) else json.dumps(raw, ensure_ascii=False)
        sql_type = col_type_map.get(col_name, str(col_type)) if col_type_map else str(col_type)
        first_rows = [str(row[col_name]) for row in sample_rows if row[col_name] is not None]
        first_rows_str = json.dumps(first_rows[:5])
        alerts = []
        if non_null == 0: alerts.append("All Missing")
        elif missing_pct > 80: alerts.append("High Nulls")
        if distinct_count == 1 and non_null > 0: alerts.append("Constant")
        if distinct_pct >= 99.5 and non_null > 1: alerts.append("Unique Key")
        if is_string and distinct_pct > 50 and distinct_count > 10: alerts.append("High Cardinality")
        if is_numeric and total_rows > 0 and (zero_count / total_rows * 100) > 50: alerts.append("Zeros")
        alert_str = ", ".join(alerts) if alerts else "OK"
        stats.append({
            "RUN_ID": run_id, "DATASET_ID": dataset_id,
            "COLUMN_ID": col_idx_offset + i + 1, "COLUMN_NAME": col_name,
            "COLUMN_DOMAIN": None,
            "DATA_TYPE": sql_type, "COLUMN_INFERRED_DATA_TYPE": sql_type,
            "COLUMN_TOTAL_COUNT": total_rows, "COLUMN_NON_NULL_COUNT": non_null,
            # "COLUMN_MISSING_COUNT": missing_count,
            "COLUMN_DISTINCT_COUNT": distinct_count,
            # "COLUMN_UNIQUE_COUNT": unique_count, "COLUMN_UNIQUE_PERCENT": unique_pct,
            # "COLUMN_IS_UNIQUE": is_unique,
            "COLUMN_ZERO_COUNT": zero_count,
            # "COLUMN_ZERO_COUNT_PERCENT": zero_pct, "COLUMN_ZERO_PERCENTAGE": zero_pct,
            # "COLUMN_NEGATIVE_COUNT": neg_count, "COLUMN_NEGATIVE_PERCENTAGE": neg_pct,
            "COLUMN_MISSING_PERCENTAGE": missing_pct, "COLUMN_DISTINCT_PERCENTAGE": distinct_pct,
            # "COLUMN_MEAN_VALUE": str(res.get(f"{prefix}__MEAN")) if is_numeric and res.get(f"{prefix}__MEAN") is not None else None,
            # "COLUMN_MEDIAN_VALUE": res.get(f"{prefix}__P50") if is_numeric else None,
            # "COLUMN_SUM_VALUE": res.get(f"{prefix}__SUM") if is_numeric else None,
            "COLUMN_MIN_VALUE": str(res[f"{prefix}__MIN"]) if res[f"{prefix}__MIN"] is not None else None,
            "COLUMN_MAX_VALUE": str(res[f"{prefix}__MAX"]) if res[f"{prefix}__MAX"] is not None else None,
            "COLUMN_AVG_LENGTH": avg_len,
            # "COLUMN_MAX_LENGTH": res.get(f"{prefix}__MAXLEN") if is_string else None,
            # "COLUMN_MIN_LENGTH": res.get(f"{prefix}__MINLEN") if is_string else None,
            # "COLUMN_MEDIAN_LENGTH": res.get(f"{prefix}__MEDLEN") if is_string else None,
            # "COLUMN_LENGTH_DISTRIBUTION": ld_str,
            "COLUMN_STD_DEV": res.get(f"{prefix}__STDDEV") if is_numeric else None,
            # "COLUMN_KURTOSIS_VALUE": res.get(f"{prefix}__KURT") if is_numeric else None,
            # "COLUMN_SKEWNESS_VALUE": res.get(f"{prefix}__SKEW") if is_numeric else None,
            "COLUMN_INVALID_DATE_COUNT": inv_date,
            # "COLUMN_INVALID_DATES_TOTAL": inv_date,
            # "COLUMN_INVALID_DATES_PERCENTAGE": inv_date_pct,
            # "COLUMN_PERCENTILE_5": res.get(f"{prefix}__P05") if is_numeric else None,
            "COLUMN_PERCENTILE_25": res.get(f"{prefix}__P25") if is_numeric else None,
            "COLUMN_PERCENTILE_50": res.get(f"{prefix}__P50") if is_numeric else None,
            "COLUMN_PERCENTILE_75": res.get(f"{prefix}__P75") if is_numeric else None,
            "COLUMN_PERCENTILE_95": res.get(f"{prefix}__P95") if is_numeric else None,
            # "COLUMN_MEMORY_SIZE_BYTES": estimated_bytes,
            # "VALUE_COUNTS_WITHOUT_NAN": vc_str,
            "COLUMN_FIRST_FEW_ROWS": first_rows_str, "COLUMN_ALERT": alert_str,
        })
    return stats

class AuditLogManager:
    def __init__(self, session, fdb, fschema, audit_table, run_id, dataset_id, ds_name, db, schema, table_name, custom_sql):
        self.session = session
        self.full_table_path = f"{q_ident(fdb)}.{q_ident(fschema)}.{q_ident(audit_table)}"
        self._q_run_id = q_str(run_id)
        self._q_dataset_id = q_str(dataset_id)
        self._q_ds_name = q_str(ds_name)
        self._q_db_name = q_str(db)
        self._q_schema_name = q_str(schema)
        self._q_table_name = q_str(table_name)
        self._q_custom_sql = q_str(custom_sql)
    def step(self, step_name):
        return AuditStep(self, step_name)

class AuditStep:
    def __init__(self, manager, step_name):
        self.mgr = manager
        self.step_name = step_name
        self._q_step_name = q_str(step_name)
    def _insert_audit(self):
        m = self.mgr
        cols = ("RUN_ID", "DATASET_ID", "DATASET_NAME", "DATABASE_NAME",
                "SCHEMA_NAME", "TABLE_NAME", "CUSTOM_SQL_QUERY", "STEP",
                "STATUS", "MESSAGE", "LOG_TIMESTAMP")
        vals = [
            m._q_run_id, m._q_dataset_id, m._q_ds_name,
            m._q_db_name, m._q_schema_name, m._q_table_name,
            m._q_custom_sql, self._q_step_name, q_str("STARTED"),
            "NULL", "CURRENT_TIMESTAMP()"
        ]
        col_str = ", ".join(cols)
        val_str = ", ".join(vals)
        return f"INSERT INTO {m.full_table_path} ({col_str}) VALUES ({val_str})"
    def _update_audit(self, status, message):
        m = self.mgr
        return (
            f"UPDATE {m.full_table_path} "
            f"SET STATUS = {q_str(status)}, "
            f"MESSAGE = {q_str(message[:4000])}, "
            f"LOG_TIMESTAMP = CURRENT_TIMESTAMP() "
            f"WHERE RUN_ID = {m._q_run_id} "
            f"AND DATASET_ID = {m._q_dataset_id} "
            f"AND STEP = {self._q_step_name} "
            f"AND STATUS = {q_str(''STARTED'')}"
        )
    def __enter__(self):
        try:
            self.mgr.session.sql(self._insert_audit()).collect()
        except Exception as e:
            print(f"AUDIT INIT FAILED for step {self.step_name}: {str(e)}")
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        status = "SUCCESS"
        message = ""
        if exc_type:
            status = "FAILURE"
            message = "".join(traceback.format_exception(exc_type, exc_val, exc_tb))
        try:
            self.mgr.session.sql(self._update_audit(status, message)).collect()
        except Exception as e:
            print(f"AUDIT UPDATE FAILED for step {self.step_name}: {str(e)}")
        if exc_type: return False
        return True

def run_profiler_native(session, run_id: str, dataset_id: str, dataset_name: str, dbname: str, schemaname: str, tablename: str, customsql: str) -> str:
    STATS_TABLE = "PROFILER_ATTRIBUTE_STATS_1"
    SUMMARY_TABLE = "PROFILER_DATASET_STATS_1"
    AUDIT_TABLE = "PROFILER_AUDIT_LOGS_1"
    framework_db = session.get_current_database().strip(''"'')
    framework_schema = session.get_current_schema().strip(''"'')
    audit = AuditLogManager(session, framework_db, framework_schema, AUDIT_TABLE,
                            run_id, dataset_id, dataset_name,
                            dbname if not customsql else None,
                            schemaname if not customsql else None,
                            tablename if not customsql else None, customsql)
    batch_size, max_async_batches = _get_warehouse_config(session)
    cached_table = None
    try:
        with audit.step("Load Data & Schema"):
            if tablename:
                path = f"{q_ident(dbname)}.{q_ident(schemaname)}.{q_ident(tablename)}"
                df = session.table(path)
                meta_db = dbname
                meta_schema = schemaname
                meta_table = tablename
            elif customsql:
                validate_custom_sql(customsql)
                random_suffix = uuid.uuid4().hex[:8]
                temp_table_name = f"TEMP_PROFILE_{run_id}_{datetime.now().strftime(''%Y%m%d%H%M%S'')}_{random_suffix}"
                temp_table_path = f"{q_ident(framework_db)}.{q_ident(framework_schema)}.{q_ident(temp_table_name)}"
                with audit.step("Create Temp Table"):
                    session.sql(f"CREATE TEMPORARY TABLE {temp_table_path} AS ({customsql})").collect()
                df = session.table(temp_table_path)
                meta_db = framework_db
                meta_schema = framework_schema
                meta_table = temp_table_name
            else:
                raise ValueError("Provide TABLENAME or CUSTOMSQL")
            if customsql:
                cached_table = df.cache_result()
                work_df = cached_table
            else:
                work_df = df
            source_table_path = f"{q_ident(meta_db)}.{q_ident(meta_schema)}.{q_ident(meta_table)}"
            sample_rows = work_df.sample(n=5).collect()
            col_type_map = _fetch_column_types(session, meta_db, meta_schema, meta_table)

        with audit.step("Calculate Native Column Stats"):
            fields = work_df.schema.fields
            metadata_row_count = None
            try:
                rc_sql = (
                    f"SELECT ROW_COUNT FROM {q_ident(meta_db)}.INFORMATION_SCHEMA.TABLES "
                    f"WHERE TABLE_SCHEMA = {q_str(meta_schema)} "
                    f"AND TABLE_NAME = {q_str(meta_table)}"
                )
                rc_res = session.sql(rc_sql).collect()
                if rc_res and rc_res[0]["ROW_COUNT"] is not None:
                    metadata_row_count = int(rc_res[0]["ROW_COUNT"])
            except Exception:
                metadata_row_count = None
            global_aggs = [
                F.approx_count_distinct(F.hash(*work_df.columns)).alias("__APPROX_UNIQ__"),
            ]
            if metadata_row_count is None:
                global_aggs.insert(0, F.count(F.lit(1)).alias("__TOTAL_ROWS__"))
            global_job = work_df.select(global_aggs).collect_nowait()

            batches = []
            # --- COMMENTED OUT: Expensive async jobs (uncomment to enable) ---
            # unique_jobs = []
            # vcounts_jobs = []
            # lendist_jobs = []
            for i in range(0, len(fields), batch_size):
                batch_fields = fields[i:i + batch_size]
                aggs, col_meta = build_col_aggs(batch_fields)
                batch_df = work_df.select(aggs)
                # uniq_sql = build_unique_count_sql(source_table_path, batch_fields)
                # uniq_job = session.sql(uniq_sql).collect_nowait() if uniq_sql else None
                # vc_sql = build_value_counts_sql(source_table_path, batch_fields)
                # vc_job = session.sql(vc_sql).collect_nowait() if vc_sql else None
                # ld_sql = build_length_dist_sql(source_table_path, batch_fields)
                # ld_job = session.sql(ld_sql).collect_nowait() if ld_job else None
                batches.append((batch_df.collect_nowait(), col_meta))
                # unique_jobs.append(uniq_job)
                # vcounts_jobs.append(vc_job)
                # lendist_jobs.append(ld_job)
                if len(batches) >= max_async_batches:
                    break

            remaining_start = min(len(batches) * batch_size, len(fields))

            global_res = global_job.result()[0]
            total_rows = metadata_row_count if metadata_row_count is not None else global_res["__TOTAL_ROWS__"]
            if total_rows == 0:
                return "SKIPPED: Empty Dataset"

            stats_output = []
            for idx, (async_job, col_meta) in enumerate(batches):
                batch_res = async_job.result()[0]
                # uniq_res = None
                # if unique_jobs[idx] is not None:
                #     try:
                #         uniq_res = unique_jobs[idx].result()[0].as_dict()
                #     except Exception:
                #         uniq_res = None
                # vc_res = None
                # if vcounts_jobs[idx] is not None:
                #     try:
                #         vc_res = vcounts_jobs[idx].result()[0].as_dict()
                #     except Exception:
                #         vc_res = None
                # ld_res = None
                # if lendist_jobs[idx] is not None:
                #     try:
                #         ld_res = lendist_jobs[idx].result()[0].as_dict()
                #     except Exception:
                #         ld_res = None
                stats_output.extend(
                    parse_batch_result(batch_res, col_meta, total_rows, sample_rows, run_id, dataset_id, idx * batch_size, col_type_map)
                )

            for i in range(remaining_start, len(fields), batch_size):
                batch_fields = fields[i:i + batch_size]
                aggs, col_meta = build_col_aggs(batch_fields)
                batch_res = work_df.select(aggs).collect()[0]
                # uniq_sql = build_unique_count_sql(source_table_path, batch_fields)
                # uniq_res = None
                # if uniq_sql:
                #     try:
                #         uniq_res = session.sql(uniq_sql).collect()[0].as_dict()
                #     except Exception:
                #         uniq_res = None
                # vc_sql = build_value_counts_sql(source_table_path, batch_fields)
                # vc_res = None
                # if vc_sql:
                #     try:
                #         vc_res = session.sql(vc_sql).collect()[0].as_dict()
                #     except Exception:
                #         vc_res = None
                # ld_sql = build_length_dist_sql(source_table_path, batch_fields)
                # ld_res = None
                # if ld_sql:
                #     try:
                #         ld_res = session.sql(ld_sql).collect()[0].as_dict()
                #     except Exception:
                #         ld_res = None
                stats_output.extend(
                    parse_batch_result(batch_res, col_meta, total_rows, sample_rows, run_id, dataset_id, i, col_type_map)
                )

        with audit.step("Calculate Dataset Summary"):
            duplicate_count = total_rows - global_res["__APPROX_UNIQ__"]
            try:
                size_sql = (
                    f"SELECT BYTES FROM {q_ident(meta_db)}.INFORMATION_SCHEMA.TABLES "
                    f"WHERE TABLE_SCHEMA = {q_str(meta_schema)} "
                    f"AND TABLE_NAME = {q_str(meta_table)}"
                )
                size_res = session.sql(size_sql).collect()
                size_gb = 0.0
                if size_res and size_res[0]["BYTES"] is not None:
                    size_gb = round(float(size_res[0]["BYTES"]) / 1024 / 1024 / 1024, 4)
            except Exception:
                size_gb = 0.0
            total_cols = len(work_df.columns)
            total_missing = sum((x["COLUMN_TOTAL_COUNT"] - x["COLUMN_NON_NULL_COUNT"]) for x in stats_output)
            total_cells = total_rows * total_cols
            null_percent = (total_missing / total_cells * 100) if total_cells > 0 else 0
            unique_values_sum = sum(x["COLUMN_DISTINCT_COUNT"] for x in stats_output)
            summary_stats = [{
                "RUN_ID": run_id, "DATASET_ID": dataset_id, "DATASET_NAME": dataset_name,
                "DATABASE_NAME": dbname, "SCHEMA_NAME": schemaname,
                "TABLE_NAME": tablename, "CUSTOM_SQL": customsql,
                "COLUMN_DOMAIN": None,
                "TOTAL_ROWS": total_rows, "TOTAL_COLUMNS": total_cols, "TABLE_SIZE_GB": size_gb,
                "MISSING_VALUES_COUNT": total_missing, "NULL_COUNT": total_missing,
                "NULL_PERCENT": null_percent, "UNIQUE_VALUES_COUNT": unique_values_sum,
                "DUPLICATE_COUNT": duplicate_count, "PROFILED_AT": datetime.now()
            }]

        with audit.step("Write Results to Table"):
            stats_path = f"{q_ident(framework_db)}.{q_ident(framework_schema)}.{q_ident(STATS_TABLE)}"
            if stats_output:
                session.create_dataframe(stats_output).write.mode("append").save_as_table(stats_path)
            summary_path = f"{q_ident(framework_db)}.{q_ident(framework_schema)}.{q_ident(SUMMARY_TABLE)}"
            if summary_stats:
                session.create_dataframe(summary_stats).write.mode("append").save_as_table(summary_path)
    finally:
        if cached_table is not None:
            try:
                cached_table.drop_table()
            except Exception:
                pass
        has_temp_table = customsql and tablename is None and cached_table is not None
        if has_temp_table:
            with audit.step("Cleanup Temp Table"):
                try:
                    session.sql(f"DROP TABLE IF EXISTS {q_ident(framework_db)}.{q_ident(framework_schema)}.{q_ident(temp_table_name)}").collect()
                except Exception as e:
                    print(f"Warning cleanup failed: {str(e)}")
    return "SUCCESS: Profiling Complete"
';
