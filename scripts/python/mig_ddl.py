import snowflake.connector
import os
import re
from config import SOURCE_CONFIG, OUTPUT_DIR_DDL

# Output Folders
FOLDERS = {
    "DDL": OUTPUT_DIR_DDL,
    "PROC": os.path.join(OUTPUT_DIR_DDL, "procedures"),
    "VIEW": os.path.join(OUTPUT_DIR_DDL, "views"),
    "FUNC": os.path.join(OUTPUT_DIR_DDL, "functions")
}

def get_connection():
    print(f"🔌 Connecting to SOURCE: {SOURCE_CONFIG['account']}...")
    return snowflake.connector.connect(
        user=SOURCE_CONFIG['user'],
        password=SOURCE_CONFIG['password'],
        account=SOURCE_CONFIG['account'],
        warehouse=SOURCE_CONFIG['warehouse'],
        database=SOURCE_CONFIG['database'],
        schema=SOURCE_CONFIG['schema'],
        role=SOURCE_CONFIG['role']
    )

def save_file(folder, filename, content):
    os.makedirs(folder, exist_ok=True)
    with open(os.path.join(folder, filename), "w", encoding="utf-8") as f:
        f.write(content.strip())
    print(f"   📄 Saved: {filename}")

def clean_ddl(text):
    """Removes Database.Schema. prefix via Regex to make DDL portable"""
    return re.sub(r'([a-zA-Z0-9_"]+\.)([a-zA-Z0-9_"]+\.)(?=[a-zA-Z0-9_"]+)', "", text)

def clean_signature(signature_string):
    """Cleans the return type from signature for GET_DDL calls"""
    if " RETURN " in signature_string:
        return signature_string.split(" RETURN ")[0]
    return signature_string

def main():
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        cur.execute(f"USE SCHEMA {SOURCE_CONFIG['database']}.{SOURCE_CONFIG['schema']}")
        
        # Buffer for Version 1.0.0 file (Sequences + Tables)
        v1_content = "USE SCHEMA {{ snowflake_schema }};\n\n"
        
        # ==========================================
        # 1. SEQUENCES
        # ==========================================
        print("\n🔢 Extracting Sequences...")
        try:
            cur.execute(f"SHOW SEQUENCES")
            # Index 6 is typically 'owner' in SHOW SEQUENCES output
            seqs = [r[0] for r in cur.fetchall() if r[6] == SOURCE_CONFIG['owner_role']]
            
            for seq in seqs:
                cur.execute(f"SELECT GET_DDL('SEQUENCE', '\"{seq}\"')")
                ddl = clean_ddl(cur.fetchone()[0])
                v1_content += f"-- Sequence: {seq}\n{ddl}\n\n"
        except Exception as e:
            print(f"   ⚠️ Error extracting sequences: {e}")

        # ==========================================
        # 2. TABLES
        # ==========================================
        print("\n📦 Extracting Tables...")
        try:
            cur.execute(f"SHOW TABLES")
            # Index 9 is typically 'owner' in SHOW TABLES output
            tables = [r[1] for r in cur.fetchall() if r[9] == SOURCE_CONFIG['owner_role']]
            
            for t in tables:
                cur.execute(f"SELECT GET_DDL('TABLE', '\"{t}\"')")
                ddl = clean_ddl(cur.fetchone()[0])
                ddl = ddl.replace("CREATE OR REPLACE TABLE", "CREATE TABLE IF NOT EXISTS")
                v1_content += f"{ddl}\n\n"
        except Exception as e:
            print(f"   ⚠️ Error extracting tables: {e}")
            
        # Save V1 file
        v1_content += "SELECT 1;" 
        save_file(FOLDERS["DDL"], "V1.0.0__initial_ddl.sql", v1_content)

        # ==========================================
        # 3. VIEWS
        # ==========================================
        print("\n👁️ Extracting Views...")
        try:
            cur.execute(f"SHOW VIEWS")
            # Index 5 is typically 'owner' in SHOW VIEWS output
            views = [r[1] for r in cur.fetchall() if r[5] == SOURCE_CONFIG['owner_role']]
            
            for v in views:
                cur.execute(f"SELECT GET_DDL('VIEW', '\"{v}\"')")
                ddl = clean_ddl(cur.fetchone()[0])
                content = "USE SCHEMA {{ snowflake_schema }};\n\n" + ddl + ";"
                save_file(FOLDERS["VIEW"], f"R__{v}.sql", content)
        except Exception as e:
             print(f"   ⚠️ Error extracting views: {e}")

        # ==========================================
        # 4. PROCEDURES
        # ==========================================
        print("\n⚙️ Extracting Procedures...")
        try:
            # Check ownership via Info Schema
            cur.execute(f"SELECT PROCEDURE_NAME FROM INFORMATION_SCHEMA.PROCEDURES WHERE PROCEDURE_OWNER = '{SOURCE_CONFIG['owner_role']}'")
            owned_procs = {r[0] for r in cur.fetchall()}
            
            # Get DDL via SHOW command (needed for signature)
            cur.execute(f"SHOW PROCEDURES")
            query_id = cur.sfqid
            cur.execute(f"""SELECT "arguments", "name" FROM TABLE(RESULT_SCAN('{query_id}')) WHERE "is_builtin" = 'N' AND "name" NOT LIKE 'SYSTEM$%'""")
            
            for row in cur.fetchall():
                raw_sig, name = row
                clean_name = name.split('(')[0].strip()
                
                if clean_name in owned_procs:
                    sig = clean_signature(raw_sig)
                    try:
                        cur.execute(f"SELECT GET_DDL('PROCEDURE', '{sig}')")
                        ddl = clean_ddl(cur.fetchone()[0])
                        content = "USE SCHEMA {{ snowflake_schema }};\n\n" + ddl
                        save_file(FOLDERS["PROC"], f"R__{clean_name}.sql", content)
                    except Exception as e:
                        print(f"   ⚠️ Failed to get DDL for Proc {name}: {e}")
        except Exception as e:
             print(f"   ⚠️ Error extracting procedures: {e}")

        # ==========================================
        # 5. FUNCTIONS
        # ==========================================
        print("\n⚡ Extracting Functions...")
        try:
            # Check ownership via Info Schema
            cur.execute(f"SELECT FUNCTION_NAME FROM INFORMATION_SCHEMA.FUNCTIONS WHERE FUNCTION_OWNER = '{SOURCE_CONFIG['owner_role']}'")
            owned_funcs = {r[0] for r in cur.fetchall()}
            
            # Get DDL via SHOW command
            cur.execute(f"SHOW FUNCTIONS")
            query_id = cur.sfqid
            cur.execute(f"""SELECT "arguments", "name" FROM TABLE(RESULT_SCAN('{query_id}')) WHERE "is_builtin" = 'N' AND "name" NOT LIKE 'SYSTEM$%'""")
            
            for row in cur.fetchall():
                raw_sig, name = row
                clean_name = name.split('(')[0].strip()
                
                if clean_name in owned_funcs:
                    sig = clean_signature(raw_sig)
                    try:
                        cur.execute(f"SELECT GET_DDL('FUNCTION', '{sig}')")
                        ddl = clean_ddl(cur.fetchone()[0])
                        content = "USE SCHEMA {{ snowflake_schema }};\n\n" + ddl 
                        save_file(FOLDERS["FUNC"], f"R__{clean_name}.sql", content)
                    except Exception as e:
                        print(f"   ⚠️ Failed to get DDL for Func {name}: {e}")
        except Exception as e:
             print(f"   ⚠️ Error extracting functions: {e}")

    finally:
        conn.close()
        print("\n✨ DDL Extraction Complete")

if __name__ == "__main__":
    main()
