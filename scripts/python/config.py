import os

# ==========================================
# 1. SOURCE CONFIGURATION (Extract From Here)
# ==========================================
# Reads from GitHub Variables: SOURCE_ACCOUNT, SOURCE_USER, etc.
# Reads from GitHub Secrets:   SOURCE_PASSWORD
SOURCE_CONFIG = {
    "user": os.getenv("SOURCE_USER"),
    "password": os.getenv("SOURCE_PASSWORD"),
    "account": os.getenv("SOURCE_ACCOUNT"),
    "warehouse": os.getenv("SOURCE_WAREHOUSE"),
    "database": os.getenv("SOURCE_DATABASE"),
    "schema": os.getenv("SOURCE_SCHEMA"),
    "role": os.getenv("SOURCE_ROLE"),
    "owner_role": os.getenv("SOURCE_OWNER_ROLE") 
}

# ==========================================
# 2. TARGET CONFIGURATION (Deploy To Here)
# ==========================================
# Reads from GitHub Variables: TARGET_ACCOUNT, TARGET_USER, etc.
# Reads from GitHub Secrets:   TARGET_PASSWORD
TARGET_CONFIG = {
    "user": os.getenv("TARGET_USER"),
    "password": os.getenv("TARGET_PASSWORD"),
    "account": os.getenv("TARGET_ACCOUNT"),
    "warehouse": os.getenv("TARGET_WAREHOUSE"),
    "database": os.getenv("TARGET_DATABASE"),
    "schema": os.getenv("TARGET_SCHEMA"),
    "role": os.getenv("TARGET_ROLE")
}

# ==========================================
# 3. GLOBAL SETTINGS
# ==========================================
# List of tables to export data from
DATA_TABLES_TO_EXPORT = ['DQ_EXPECTATION_MASTER'] 

# Dynamic Paths: Calculates folders based on where this script is located
# This ensures it works on both Windows (Local) and Linux (GitHub Actions)
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Destination folders for generated files
OUTPUT_DIR_DDL = os.path.join(REPO_ROOT, "scripts", "ddl")
OUTPUT_DIR_DATA = os.path.join(REPO_ROOT, "data_exports")