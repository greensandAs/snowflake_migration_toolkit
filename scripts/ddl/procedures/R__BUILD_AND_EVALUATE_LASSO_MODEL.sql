USE DATABASE {{ snowflake_database }};

USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE PROCEDURE "BUILD_AND_EVALUATE_LASSO_MODEL"(
    "P_TABLE_NAME" VARCHAR,
    "P_FEATURES" ARRAY,
    "P_TARGET" VARCHAR,
    "P_RELATIONS" ARRAY,
    "P_DATA_CONFIG" VARCHAR,
    "P_COMPUTED_VAR" VARCHAR,
    "P_CODE" VARCHAR,
    "P_REL" VARCHAR
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('pandas', 'numpy', 'scikit-learn', 'snowflake-snowpark-python')
HANDLER = 'build_lasso_with_relations_handler'
EXECUTE AS CALLER
AS '
import snowflake.snowpark as sp
from snowflake.snowpark import Session
import pandas as pd
import numpy as np
import json
from sklearn.linear_model import LassoCV
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_absolute_percentage_error

def build_lasso_with_relations_handler(session: Session, p_table_name: str, p_features: list, p_target: str,
                                       p_relations: list, p_data_config: str, p_computed_var: str, p_code: str, p_rel: str):
    """
    Build Lasso regression model with relationship tracking on sample data.
    This mirrors the _generate_lasso_equations() function with feature engineering.
    """
    try:
        print(f"Loading data from {p_table_name}")

        # Load full data
        df = session.table(p_table_name).to_pandas()

        if df.empty:
            return json.dumps({"source": "Lasso Regression with Relations", "error": "Dataset is empty"})

        # Sample data (same logic as original Python)
        sample_size = min(len(df), max(10000, int(len(df) * 0.2)))
        df_sample = df.sample(min(sample_size, len(df)), random_state=42)

        print(f"Loaded {len(df)} rows, using sample of {len(df_sample)} rows")

        # Parse config
        try:
            config = json.loads(p_data_config)
        except:
            config = {"relative_tolerance": 0.05, "r2_threshold": 70, "mape_threshold": 20}

        # Prepare features and target
        base_features = [f for f in p_features if f in df_sample.columns]

        if p_target not in df_sample.columns or not base_features:
            return json.dumps({"source": "Lasso Regression with Relations", "error": "Missing target or features"})

        feature_df = df_sample[base_features].copy()
        feature_names = list(base_features)

        # Add interaction terms (if limited features)
        if len(base_features) <= 5:
            for i, col1 in enumerate(base_features):
                for col2 in base_features[i + 1:]:
                    name = f"{col1}*{col2}"
                    feature_df[name] = df_sample[col1] * df_sample[col2]
                    feature_names.append(name)

        # Add ratio terms (if limited features)
        if len(base_features) <= 4:
            for i, col1 in enumerate(base_features):
                for col2 in base_features[i + 1:]:
                    name = f"{col1}/{col2}"
                    with np.errstate(divide="ignore", invalid="ignore"):
                        ratio = df_sample[col1] / df_sample[col2]
                    feature_df[name] = ratio.replace([np.inf, -np.inf], np.nan)
                    feature_names.append(name)

        feature_df[p_target] = df_sample[p_target]

        # Train Lasso
        clean_df = feature_df.dropna()
        if len(clean_df) < 10:
            return json.dumps({"source": "Lasso Regression with Relations", "error": "Insufficient clean data"})

        X = clean_df[feature_names]
        y = clean_df[p_target]

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

        model = LassoCV(cv=5, random_state=42)
        model.fit(X_train, y_train)

        y_pred_test = model.predict(X_test)
        r2_test = round(float(r2_score(y_test, y_pred_test)) * 100, 2)
        mape_test = round(float(mean_absolute_percentage_error(y_test, y_pred_test)) * 100, 2)

        # Build equation
        intercept = model.intercept_
        equation_parts = [f"{intercept:.4f}"]
        for coef, feat in zip(model.coef_, feature_names):
            if abs(coef) > 1e-6:
                equation_parts.append(f"({coef:.4f} * {feat})")

        if len(equation_parts) == 1:
            eqn = f"{p_target} = {intercept:.4f}"
        else:
            eqn = f"{p_target} = " + " + ".join(equation_parts)

        # Calculate accuracy with tolerance
        y_pred_all = model.predict(X)
        tolerance = config.get("relative_tolerance", 0.05) * np.std(y)
        accuracy = round(float((np.abs(y - y_pred_all) <= tolerance).sum() / len(y)) * 100, 2)

        result = {
            "source": "Lasso Regression with Relations",
            "relation_type": p_rel,
            "target": p_target,
            "code": p_code,
            "equation": eqn,
            "r2_test": r2_test,
            "mape_test": mape_test,
            "accuracy": accuracy,
            "intercept": round(float(intercept), 6),
            "coefficients": {str(feat): round(float(coef), 6) for feat, coef in zip(feature_names, model.coef_)},
            "features": base_features,
            "data_points": len(clean_df),
            "sample_size": len(df_sample),
            "total_rows": len(df)
        }

        print(f"Lasso with relations: {eqn}")
        print(f"Metrics - R²={r2_test}, MAPE={mape_test}, Accuracy={accuracy}")

        return json.dumps(result)

    except Exception as e:
        error_msg = f"Error in build_lasso_with_relations: {str(e)}"
        print(error_msg)
        return json.dumps({"source": "Lasso Regression with Relations", "error": error_msg})
';
