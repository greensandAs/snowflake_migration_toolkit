import snowflake.connector
import os
from config import SOURCE_CONFIG, DATA_TABLES_TO_EXPORT, OUTPUT_DIR_DATA

TEMP_STAGE = "MIGRATION_EXPORT_STAGE"

def main():
    print(f"🔌 Connecting to SOURCE for Data Export: {SOURCE_CONFIG['account']}...")
    conn = snowflake.connector.connect(
        user=SOURCE_CONFIG['user'], 
        password=SOURCE_CONFIG['password'], 
        account=SOURCE_CONFIG['account'],
        warehouse=SOURCE_CONFIG['warehouse'], 
        database=SOURCE_CONFIG['database'], 
        schema=SOURCE_CONFIG['schema'], 
        role=SOURCE_CONFIG['role']
    )
    cur = conn.cursor()
    
    # Ensure export directory exists
    os.makedirs(OUTPUT_DIR_DATA, exist_ok=True)
    # Absolute path is required for the Snowflake GET command
    abs_path = os.path.abspath(OUTPUT_DIR_DATA) 

    try:
        cur.execute(f"USE SCHEMA {SOURCE_CONFIG['database']}.{SOURCE_CONFIG['schema']}")
        
        print("🔨 Creating Temporary Stage...")
        cur.execute(f"""
            CREATE OR REPLACE TEMPORARY STAGE {TEMP_STAGE} 
            FILE_FORMAT=(TYPE='CSV' COMPRESSION='GZIP' FIELD_OPTIONALLY_ENCLOSED_BY='"')
        """)

        for table in DATA_TABLES_TO_EXPORT:
            print(f"📦 Exporting Table: {table}")
            filename = f"{table}.csv.gz"
            
            # 1. Unload Data (Server-Side)
            print(f"   -> Unloading to Stage...")
            # SINGLE=TRUE is fine for config tables. For massive data tables >5GB, remove it.
            cur.execute(f"""
                COPY INTO @{TEMP_STAGE}/{filename} 
                FROM "{table}" 
                HEADER=TRUE 
                SINGLE=TRUE 
                OVERWRITE=TRUE
            """)
            
            # 2. Download Data (Stage -> Local)
            print(f"   -> Downloading to {abs_path}...")
            cur.execute(f"GET @{TEMP_STAGE}/{filename} file://{abs_path}")
            
    finally:
        conn.close()
        print("\n✨ Data Export Complete")

if __name__ == "__main__":
    main()