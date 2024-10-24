import pandas as pd
from extract.extract_call_data import extract_call_data  
from extract.extract_network_usage import extract_network_usage

def integrate_call_network(call_data, network_data):
    df_call = pd.DataFrame(call_data, columns=['caller', 'duration', 'timestamp'])  
    df_network = pd.DataFrame(network_data, columns=['user_id', 'network_type'])  

    
    integrated_data = pd.merge(df_call, df_network, left_on='caller', right_on='user_id')

    return integrated_data

if __name__ == "__main__":
    call_data = extract_call_data()  
    network_data = extract_network_usage()  
    integrated_data = integrate_call_network(call_data, network_data)
    print(f"Integrated {len(integrated_data)} records.")
