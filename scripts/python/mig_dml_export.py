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
    
    # Ensure export directory exists locally
    os.makedirs(OUTPUT_DIR_DATA, exist_ok=True)
    
    # FIX 1: Get absolute path and force forward slashes (Crucial for Snowflake GET)
    abs_path = os.path.abspath(OUTPUT_DIR_DATA).replace('\\', '/')

    try:
        cur.execute(f"USE SCHEMA {SOURCE_CONFIG['database']}.{SOURCE_CONFIG['schema']}")
        
        print("🔨 Creating Temporary Stage...")
        cur.execute(f"""
            CREATE OR REPLACE TEMPORARY STAGE {TEMP_STAGE} 
            FILE_FORMAT=(TYPE='CSV' COMPRESSION='GZIP' FIELD_OPTIONALLY_ENCLOSED_BY='"')
        """)

        for table in DATA_TABLES_TO_EXPORT:
            print(f"\n📦 Exporting Table: {table}")
            filename = f"{table}.csv.gz"
            
            # 1. Unload Data (Server-Side)
            print(f"   -> Unloading to Stage @{TEMP_STAGE}...")
            cur.execute(f"""
                COPY INTO @{TEMP_STAGE}/{filename} 
                FROM "{table}" 
                HEADER=TRUE 
                SINGLE=TRUE 
                OVERWRITE=TRUE
            """)
            
            # 2. Download Data (Stage -> Local)
            # FIX 2: Added the trailing slash '/' after {abs_path}
            print(f"   -> Downloading to file://{abs_path}/...")
            cur.execute(f"GET @{TEMP_STAGE}/{filename} file://{abs_path}/")
            
            # 3. Print the results to the console/GitHub Actions log
            download_results = cur.fetchall()
            if not download_results:
                print("   ⚠️ WARNING: GET command executed, but no files were downloaded!")
            else:
                for row in download_results:
                    # row format: [file_name, file_size, return_status, message]
                    print(f"      ✅ Downloaded: {row[0]} | Status: {row[2]} | Size: {row[1]} bytes")
            
    except Exception as e:
        print(f"\n❌ Error during export: {e}")
        exit(1) # Force the pipeline to fail if there's an error
            
    finally:
        conn.close()
        print("\n✨ Data Export Complete")

if __name__ == "__main__":
    main()
