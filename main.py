import pandas as pd
from load.load_to_data_warehouse import load_to_data_warehouse
from extract.extract_call_data import extract_call_data
from extract.extract_network_usage import extract_network_usage  
from integration.integrate_call_network_data import integrate_call_network

def get_user_input():
    print("Enter the following call details:")
    caller = input("Caller Number: ")
    receiver = input("Receiver Number: ")
    duration = input("Duration (in seconds): ")
    timestamp = input("Timestamp (YYYY-MM-DD HH:MM:SS): ")
    
    
    data = {
        'call_id': [None],  
        'caller': [caller],
        'receiver': [receiver],
        'duration': [duration],
        'timestamp': [timestamp]
    }
    
    return pd.DataFrame(data)

if __name__ == "__main__":
    
    user_data = get_user_input()
    
    
    load_to_data_warehouse(user_data, 'call_records_warehouse')

    
    call_data = extract_call_data()
    print(f"Extracted {len(call_data)} records from call_records.")

    
    network_data = extract_network_usage()  
    
    
    integrated_data = integrate_call_network(call_data, network_data)
    print(f"Integrated {len(integrated_data)} records.")
