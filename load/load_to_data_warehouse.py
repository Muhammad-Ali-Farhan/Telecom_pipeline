
import psycopg2
import pandas as pd
from config.config import DB_CREDENTIALS, WAREHOUSE_CREDENTIALS

def load_to_data_warehouse(caller, receiver, duration, timestamp):
    
    data = {
        'caller': [caller],
        'receiver': [receiver],
        'duration': [duration],
        'timestamp': [timestamp]
    }
    df = pd.DataFrame(data)

    
    conn = psycopg2.connect(**WAREHOUSE_CREDENTIALS)
    cursor = conn.cursor()

    for index, row in df.iterrows():
        cursor.execute(
            "INSERT INTO call_records_warehouse (caller, receiver, duration, timestamp) VALUES (%s, %s, %s, %s)",
            (row['caller'], row['receiver'], row['duration'], row['timestamp'])
        )

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Loaded {len(df)} records into call_records_warehouse.")

def fetch_data_from_warehouse():
    conn = psycopg2.connect(**WAREHOUSE_CREDENTIALS)
    query = "SELECT * FROM call_records_warehouse"  # Adjust the table name if needed
    df = pd.read_sql(query, conn)  # Use pandas to fetch data directly into a DataFrame
    conn.close()
    return df