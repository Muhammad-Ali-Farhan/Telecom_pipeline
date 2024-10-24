import psycopg2
from config.config import DB_CREDENTIALS, WAREHOUSE_CREDENTIALS

def migrate_data():
    source_conn = psycopg2.connect(**DB_CREDENTIALS)
    target_conn = psycopg2.connect(**WAREHOUSE_CREDENTIALS)
    
    source_cursor = source_conn.cursor()
    target_cursor = target_conn.cursor()
    
    source_cursor.execute("SELECT * FROM call_records")
    data = source_cursor.fetchall()
    
    for row in data:
        target_cursor.execute("""
        INSERT INTO call_records_warehouse (call_id, caller, receiver, duration, timestamp)
        VALUES (%s, %s, %s, %s, %s)
        """, row)
    
    target_conn.commit()
    source_cursor.close()
    target_cursor.close()
    source_conn.close()
    target_conn.close()

if __name__ == "__main__":
    migrate_data()
    print("Data migration completed.")
