-- Set the context (Ensure the session is pointed to the correct location)
USE DATABASE {{ snowflake_database }};
USE SCHEMA {{ snowflake_schema }};

-- DDL for PROCEDURE: DQ_FRAMEWORK.METADATA.GENERATE_NUMERIC_CORRELATION_RULES(VARCHAR, ARRAY, FLOAT)
-- Extracted on: 2025-10-27 10:25:01
--------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE GENERATE_NUMERIC_CORRELATION_RULES(
    p_table_name VARCHAR,
    p_columns ARRAY,
    p_threshold FLOAT
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('pandas', 'numpy', 'snowflake-snowpark-python')
HANDLER = 'generate_correlation_rules_handler'
EXECUTE AS OWNER
AS '
import snowflake.snowpark as sp
from snowflake.snowpark import Session
import pandas as pd
import numpy as np
import json

def generate_correlation_rules_handler(session: Session, p_table_name: str, p_columns: list, p_threshold: float):
    """
    Generate correlation rules from sample data.
    Uses sample data like the original Python function in numerical_relationships.py
    """
    try:
        print(f"Loading data from {p_table_name}")

        # Load full dataset
        df = session.table(p_table_name).to_pandas()

        if df.empty:
            return json.dumps({"status": "success", "message": "Dataset is empty", "correlations": [], "perfect_correlations": []})

        # Filter to only numeric columns that exist in the table
        numeric_cols = [col for col in p_columns if col in df.columns]

        if not numeric_cols:
            return json.dumps({"status": "success", "message": "No numeric columns found", "correlations": [], "perfect_correlations": []})

        # Sample data (same logic as original Python function)
        sample_size = min(len(df), max(10000, int(len(df) * 0.2)))
        df_sample = df[numeric_cols].sample(min(sample_size, len(df)), random_state=42)

        print(f"Loaded {len(df)} rows, using sample of {len(df_sample)} rows")
        print(f"Processing {len(numeric_cols)} numerical columns")

        # Calculate Pearson correlation matrix
        corr_matrix = df_sample[numeric_cols].corr(method="pearson")
        print(f"Correlation matrix computed")

        perfect_correlations = []
        processed = set()
        eps = 1e-6

        for col in numeric_cols:
            if col in processed or col not in corr_matrix.columns:
                continue

            # Find columns where absolute correlation is near 1.0
            perfect_matches = corr_matrix[col][(corr_matrix[col].abs() >= 0.9) & (corr_matrix.index != col)]

            if perfect_matches.empty:
                continue

            # Identify the group and mark as processed
            group_cols = [col] + list(perfect_matches.index)
            processed.update(group_cols)

            # Derive equations (Logic from _derive_perfect_corr_equations)
            anchor = group_cols[0]
            for target_col in group_cols[1:]:
                common = df_sample[[anchor, target_col]].dropna()
                if len(common) < 2:
                    continue

                x, y = common[anchor].values, common[target_col].values
                std_x, std_y = np.std(x), np.std(y)

                if std_x == 0: continue

                a = std_y / std_x
                b = y.mean() - a * x.mean()

                # Determine Relationship Type
                if abs(a - 1.0) < eps and abs(b) < eps:
                    eqn, rel_type = f"{target_col} = {anchor}", "direct"
                elif abs(a + 1.0) < eps and abs(b) < eps:
                    eqn, rel_type = f"{target_col} = -{anchor}", "inverse"
                elif abs(b) < eps:
                    eqn, rel_type = f"{target_col} = {a:.6f} * {anchor}", "scaled"
                else:
                    sign = "+" if b > 0 else "-"
                    eqn, rel_type = f"{target_col} = {a:.6f} * {anchor} {sign} {abs(b):.6f}", "scaled"

                perfect_correlations.append({
                    "target_variable": target_col,
                    "feature_variables": [anchor],
                    "equation": eqn,
                    "reason": "Perfect correlation",
                    "source": "perfect_correlation",
                    "slope": round(float(a), 6),
                    "intercept": round(float(b), 6),
                    "relationship_type": rel_type,
                    "perfect_r2": 1.0
                })

        # --- STEP 2: Non-perfect correlations above threshold ---
        correlations = []
        for col in numeric_cols:
            # Filter columns that are related but not "Perfect" (already processed)
            related = corr_matrix[col][
                (corr_matrix[col].abs() >= p_threshold) &
                (corr_matrix[col].abs() < 0.9999) &
                (corr_matrix.index != col)
            ]

            if not related.empty:
                features = [{"feature": feat, "correlation": round(float(val), 4)} for feat, val in related.items()]
                correlations.append({
                    "target_variable": col,
                    "correlated_features": features
                })

        return json.dumps({
            "status": "success",
            "message": f"Found {len(correlations)} correlated sets and {len(perfect_correlations)} perfect equations",
            "correlations": correlations,
            "perfect_correlations": perfect_correlations,
            "threshold_used": p_threshold,
            "sample_size": len(df_sample)
        })

    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)})
';
