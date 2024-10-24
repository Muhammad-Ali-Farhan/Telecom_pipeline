from flask import Flask, render_template, request
from load.load_to_data_warehouse import load_to_data_warehouse
from extract.extract_call_data import extract_call_data
from extract.extract_network_usage import extract_network_usage
import pandas as pd
import psycopg2
from config.config import WAREHOUSE_CREDENTIALS

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    message = ""
    if request.method == 'POST':
        caller = request.form['caller']
        receiver = request.form['receiver']
        duration = request.form['duration']
        timestamp = request.form['timestamp']

        # Load the data into the warehouse
        load_to_data_warehouse(caller, receiver, duration, timestamp)
        message = "Data has been successfully saved to the warehouse."

    return render_template('index.html', message=message)

@app.route('/data', methods=['GET'])
def view_data():
    # Connect to the warehouse and fetch data
    conn = psycopg2.connect(**WAREHOUSE_CREDENTIALS)
    query = "SELECT * FROM call_records_warehouse"
    df = pd.read_sql_query(query, conn)
    conn.close()

    # Convert DataFrame to HTML
    data_html = df.to_html(classes='data', index=False)

    return render_template('view_data.html', data=data_html)

if __name__ == '__main__':
    app.run(debug=True)
