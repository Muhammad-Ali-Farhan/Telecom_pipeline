import pandas as pd

def clean_call_data(call_data):
    df = pd.DataFrame(call_data, columns=['call_id', 'caller', 'receiver', 'duration', 'timestamp'])
    
    
    df = df.dropna()
    df['duration'] = df['duration'].apply(lambda x: max(0, x))  
    
    return df

if __name__ == "__main__":
    call_data = [(1, 'John', 'Alice', 300, '2024-10-22 12:00:00')]  
    clean_data = clean_call_data(call_data)
    print(clean_data)
