-- Set the context (Ensure the session is pointed to the correct  location)

USE DATABASE {{ snowflake_database }};
USE SCHEMA {{ snowflake_schema }};


CREATE OR REPLACE PROCEDURE VALIDATE_EQUATIONS_ON_DATA(
    p_table_name VARCHAR,
    p_equations VARCHAR,
    p_tolerance FLOAT
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('pandas', 'numpy', 'snowflake-snowpark-python')
HANDLER = 'validate_equations_handler'
EXECUTE AS CALLER
AS '
import snowflake.snowpark as sp
from snowflake.snowpark import Session
import pandas as pd
import numpy as np
import json
import re

def validate_equations_handler(session: Session, p_table_name: str, p_equations: str, p_tolerance: float):
    """
    Validate mathematical equations against sample data.
    This mirrors the validate_equation_on_data() function from numerical_relationships.py
    """
    try:
        print(f"Loading data from {p_table_name} for equation validation")

        # Load full data
        df = session.table(p_table_name).to_pandas()

        if df.empty:
            return json.dumps({"validated_equations": [], "error": "Dataset is empty"})

        # Sample data (same logic as original Python)
        sample_size = min(len(df), max(10000, int(len(df) * 0.2)))
        df_sample = df.sample(min(sample_size, len(df)), random_state=42)

        print(f"Loaded {len(df)} rows, using sample of {len(df_sample)} rows for validation")

        # Parse input JSON
        try:
            equations_list = json.loads(p_equations)
        except:
            return json.dumps({"validated_equations": [], "error": "Failed to parse equations JSON"})

        validation_results = []

        # Validate each equation (logic from original Python function)
        for equation in equations_list:
            try:
                print(f"Validating equation: {equation}")

                # Parse equation: "target = expression"
                if "=" not in equation:
                    validation_results.append({
                        "equation": equation,
                        "valid": False,
                        "satisfaction_rate": 0.0,
                        "total_records": 0,
                        "satisfied_records": 0,
                        "error_message": "Invalid equation format (no equals sign)"
                    })
                    continue

                parts = equation.split("=", 1)
                if len(parts) < 2:
                    validation_results.append({
                        "equation": equation,
                        "valid": False,
                        "satisfaction_rate": 0.0,
                        "total_records": 0,
                        "satisfied_records": 0,
                        "error_message": "Invalid equation format"
                    })
                    continue

                target_col = parts[0].strip()
                expression = parts[1].strip()

                # Check if target column exists
                if target_col not in df_sample.columns:
                    validation_results.append({
                        "equation": equation,
                        "valid": False,
                        "satisfaction_rate": 0.0,
                        "total_records": len(df_sample),
                        "satisfied_records": 0,
                        "error_message": f"Target column ''{target_col}'' not found"
                    })
                    continue

                # Remove NaN values
                needed_cols = {target_col}

                # find referenced columns from the expression
                tokens = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", expression)
                for t in tokens:
                    if t in df_sample.columns:
                        needed_cols.add(t)

                clean_df = df_sample.dropna(subset=list(needed_cols))

                if clean_df.empty:
                    validation_results.append({
                        "equation": equation,
                        "valid": False,
                        "satisfaction_rate": 0.0,
                        "total_records": len(df_sample),
                        "satisfied_records": 0,
                        "error_message": "No valid records (all NaN)"
                    })
                    continue

                # Build column mapping and evaluation context
                col_mapping = {}
                eval_context = {"np": np}

                for col in clean_df.columns:
                    # Create safe column name
                    safe_name = "".join(c if c.isalnum() or c == "_" else "_" for c in col)
                    if safe_name and safe_name[0].isdigit():
                        safe_name = "_" + safe_name
                    col_mapping[col] = safe_name
                    eval_context[safe_name] = clean_df[col].values

                print(f"Column mapping: {col_mapping}")

                # Sanitize expression by replacing column names with safe names
                expr_sanitized = expression
                for orig_col, safe_name in col_mapping.items():
                    # Use word boundaries to avoid partial replacements
                    expr_sanitized = re.sub(
                        r"\b" + re.escape(orig_col) + r"\b",
                        safe_name,
                        expr_sanitized
                    )

                print(f"Sanitized expression: {expr_sanitized}")

                # Evaluate expression
                try:
                    predicted_values = eval(expr_sanitized, {"__builtins__": {}}, eval_context)
                except Exception as e:
                    validation_results.append({
                        "equation": equation,
                        "valid": False,
                        "satisfaction_rate": 0.0,
                        "total_records": len(clean_df),
                        "satisfied_records": 0,
                        "error_message": f"Expression evaluation error: {str(e)}"
                    })
                    continue

                # Convert predicted values to numpy array
                if isinstance(predicted_values, np.ndarray):
                    predicted = predicted_values
                elif isinstance(predicted_values, pd.Series):
                    predicted = predicted_values.values
                else:
                    predicted = np.array([predicted_values] * len(clean_df))

                # Get actual values
                actual = clean_df[target_col].values

                # Handle shape mismatch
                if predicted.shape != actual.shape:
                    try:
                        predicted = np.broadcast_to(predicted, actual.shape)
                    except:
                        validation_results.append({
                            "equation": equation,
                            "valid": False,
                            "satisfaction_rate": 0.0,
                            "total_records": len(clean_df),
                            "satisfied_records": 0,
                            "error_message": "Expression result shape mismatch"
                        })
                        continue

                # Convert to float for comparison
                actual_float = np.array(actual, dtype=float)
                predicted_float = np.array(predicted, dtype=float)

                # Calculate threshold tolerance
                threshold = p_tolerance * np.abs(actual_float)
                threshold = np.maximum(threshold, 1e-6)

                # Check satisfaction
                satisfied = np.abs(actual_float - predicted_float) <= threshold
                satisfaction_rate = (satisfied.sum() / len(satisfied)) * 100 if len(satisfied) > 0 else 0
                is_valid = satisfaction_rate >= 90.0

                validation_results.append({
                    "equation": equation,
                    "valid": bool(is_valid),
                    "satisfaction_rate": float(round(satisfaction_rate, 2)),
                    "total_records": int(len(clean_df)),
                    "satisfied_records": int(satisfied.sum()),
                    "error_message": None if is_valid else f"Only {satisfaction_rate:.2f}% records satisfy the equation"
                })

                print(f"Validation result: {satisfaction_rate:.2f}% satisfaction, Valid: {is_valid}")

            except Exception as e:
                print(f"Error validating equation: {str(e)}")
                validation_results.append({
                    "equation": equation,
                    "valid": False,
                    "satisfaction_rate": 0.0,
                    "total_records": 0,
                    "satisfied_records": 0,
                    "error_message": f"Validation error: {str(e)}"
                })

        result = {
            "validated_equations": validation_results,
            "total_validated": len(equations_list),
            "tolerance_used": p_tolerance
        }

        return json.dumps(result)

    except Exception as e:
        error_msg = f"Error in validate_equations: {str(e)}"
        print(error_msg)
        return json.dumps({"validated_equations": [], "error": error_msg})
';
