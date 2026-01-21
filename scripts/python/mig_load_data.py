import snowflake.connector
import os
import glob
from config import TARGET_CONFIG, DATA_TABLES_TO_EXPORT, OUTPUT_DIR_DATA

TARGET_STAGE = "DATA_LOAD_STAGE"

def main():
    print(f"🔌 Connecting to TARGET for Data Load: {TARGET_CONFIG['account']}...")
    conn = snowflake.connector.connect(
        user=TARGET_CONFIG['user'], 
        password=TARGET_CONFIG['password'], 
        account=TARGET_CONFIG['account'],
        warehouse=TARGET_CONFIG['warehouse'], 
        database=TARGET_CONFIG['database'], 
        schema=TARGET_CONFIG['schema'], 
        role=TARGET_CONFIG['role']
    )
    cur = conn.cursor()

    try:
        cur.execute(f"USE SCHEMA {TARGET_CONFIG['database']}.{TARGET_CONFIG['schema']}")
        
        # 1. Create Stage (Persistent)
        print(f"🔨 Ensuring Stage {TARGET_STAGE} exists...")
        cur.execute(f"""
            CREATE STAGE IF NOT EXISTS {TARGET_STAGE} 
            FILE_FORMAT=(TYPE='CSV' COMPRESSION='GZIP')
        """)

        # 2. Find Exported Files
        files = glob.glob(os.path.join(OUTPUT_DIR_DATA, "*.csv.gz"))
        print(f"📂 Found {len(files)} files to process.")
        
        for filepath in files:
            filename = os.path.basename(filepath)
            # Infer table name from filename (e.g., 'DQ_RULE_CONFIG.csv.gz' -> 'DQ_RULE_CONFIG')
            table_name = filename.replace(".csv.gz", "")
            
            if table_name not in DATA_TABLES_TO_EXPORT:
                print(f"   ⚠️ Skipping {filename} (Not in config list)")
                continue

            print(f"🚀 Processing {table_name}...")

            # A. Upload to 'not_processed' folder in Stage
            print(f"   1. Uploading to @{TARGET_STAGE}/not_processed/")
            # Windows/Linux path handling for PUT command
            # AUTO_COMPRESS=FALSE because files are already .gz from Source export
            put_sql = f"PUT file://{filepath} @{TARGET_STAGE}/not_processed/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            cur.execute(put_sql)

            # B. Load into Target Table
            print(f"   2. Loading into Table...")
            try:
                copy_sql = f"""
                    COPY INTO "{table_name}" 
                    FROM @{TARGET_STAGE}/not_processed/{filename}
                    FILE_FORMAT = (
                        TYPE='CSV' 
                        SKIP_HEADER=1 
                        FIELD_OPTIONALLY_ENCLOSED_BY='"'
                        NULL_IF=('NULL', 'nan')
                    )
                    ON_ERROR = 'CONTINUE'
                """
                cur.execute(copy_sql)
                print("      ✅ Load Success")
                
                # C. "Move" to Processed (Remove from not_processed)
                # Since we cannot effectively 'move' files between internal stage folders instantly,
                # removing it from 'not_processed' confirms it has been consumed.
                print(f"   3. Cleaning up 'not_processed'...")
                cur.execute(f"REMOVE @{TARGET_STAGE}/not_processed/{filename}")
                
            except Exception as e:
                print(f"      ❌ Load Failed: {e}")

    finally:
        conn.close()
        print("\n✨ Data Load Complete")

if __name__ == "__main__":
    main()