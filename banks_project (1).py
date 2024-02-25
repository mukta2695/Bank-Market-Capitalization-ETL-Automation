import requests
import pandas as pd
import numpy as np
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime

# Importing the required libraries

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name', 'MC_USD_Billion'] # Initially While extraction
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'
exchange_rate = './exchange_rate.csv'

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Month-Day-Hr-Min-Sec
    now = datetime.now() # Current Time
    timestamp = now.strftime(timestamp_format)
    with open("./code_log.txt", "a") as f:
        f.write(timestamp + ':' + message + '\n')

def extract(url, table_attribs):
    html_page = requests.get(url).text   # Get HTML Page
    soup = BeautifulSoup(html_page, 'html.parser') # Parsing the text into object 
    df = pd.DataFrame(columns = table_attribs) # Create an empty pandas DataFrame
    tables = soup.find('tbody')     # Extract all 'tbody' attributes of the HTML object.
    rows = tables.find_all('tr') 
    for row in rows:  # Check the contents of each row, having attribute ‘td’, for the following conditions.
        col = row.find_all('td')
        if len(col) != 0:
            col_data  = col[1].find_all('a')[1]
            if col_data is not None:
                data_dict = {
                    "Name" : col_data.contents[0],
                    "MC_USD_Billion" : col[2].contents[0]                
                }
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index = True)
        MC_USD_list = df['MC_USD_Billion'].tolist()  # Convert df to list
        for i in range(len(MC_USD_list)):
            mc_usd_val = str(MC_USD_list[i]) 
            MC_USD_list[i] = float(''.join(mc_usd_val.split('\n'))) # Removing \n
        df['MC_USD_Billion'] = MC_USD_list
    #print(df)
    return df

def transform(df, csv_path):
    csvfile = pd.read_csv(csv_path)
    csv_dict = csvfile.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x * csv_dict['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * csv_dict['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * csv_dict['INR'],2) for x in df['MC_USD_Billion']]
    #print(df['MC_EUR_Billion'][4])
    #print(df)
    return df

def load_to_csv(df, output_path):
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index = False)

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


log_progress('Preliminaries complete. Initiating ETL process')

extracted_df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')

transformed_df = transform(extracted_df, exchange_rate)
log_progress('Data transformation complete. Initiating Loading process')

load_to_csv(transformed_df, csv_path)
log_progress('Data saved to CSV file')

conn = sqlite3.connect(db_name)
log_progress('SQL Connection initiated')
load_to_db(transformed_df, conn, table_name)
log_progress('Data loaded to Database as a table, Executing queries')

queries = ['SELECT * FROM Largest_banks', 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks', 'SELECT Name from Largest_banks LIMIT 5']

for query_statement in queries:
    run_query(query_statement, conn)
    
log_progress('Process Complete')

conn.close()
log_progress('Server Connection closed')

