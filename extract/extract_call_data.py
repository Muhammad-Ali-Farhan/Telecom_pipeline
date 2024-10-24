import psycopg2
import pandas as pd  
from config.config import DB_CREDENTIALS

def extract_call_data():
    conn = psycopg2.connect(**DB_CREDENTIALS)
    cursor = conn.cursor()
    
    query = "SELECT * FROM call_records"
    cursor.execute(query)
    data = cursor.fetchall()

    
    columns = [desc[0] for desc in cursor.description]  
    df = pd.DataFrame(data, columns=columns)  

    cursor.close()
    conn.close()
    
    return df  

if __name__ == "__main__":
    call_data = extract_call_data()
    print(f"Extracted {len(call_data)} records.")
