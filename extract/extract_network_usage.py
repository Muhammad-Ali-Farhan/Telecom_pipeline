import psycopg2
import pandas as pd
from config.config import DB_CREDENTIALS

def extract_network_usage():
    conn = psycopg2.connect(**DB_CREDENTIALS)
    cursor = conn.cursor()
    
    query = "SELECT * FROM network_usage"  
    cursor.execute(query)
    data = cursor.fetchall()
    
    
    columns = [desc[0] for desc in cursor.description]  
    df = pd.DataFrame(data, columns=columns)  
    
    cursor.close()
    conn.close()
    
    return df  
