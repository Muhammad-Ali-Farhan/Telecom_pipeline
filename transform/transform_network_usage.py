import pandas as pd

def transform_network_usage(network_data):
    df = pd.DataFrame(network_data, columns=['user_id', 'data_used', 'timestamp'])
    
    
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    
    df_agg = df.groupby('user_id').agg({'data_used': 'sum'}).reset_index()
    
    return df_agg

if __name__ == "__main__":
    network_data = [(1, 500, '2024-10-22 12:00:00'), (1, 300, '2024-10-23 14:00:00')]  
    transformed_data = transform_network_usage(network_data)
    print(transformed_data)
