# Code for ETL operations on Country-GDP data

# Importing the required libraries

import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from datetime import datetime
import lxml
from sqlalchemy import create_engine


# My Variables

url = "https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks"
csv_path_file = "./Largest_banks_data.csv"
table_attribs = ["Name", "MC_USD_Billion"]
table_attribs_final = ["Name", "MC_USD_Billion", "MC_GBP_Billion", "MC_EUR_Billion", "MC_INR_Billion"]
database = "Banks.db"
table_name = "Largest_banks"
log_file = "code_log.txt"
Exchange_rate_CSV_path = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"


def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''

    time_stamp = datetime.now().strftime(r"%Y-%m-%d %H:%M:%S")
    log_line = f"{time_stamp} : {message}\n"

    with open (log_file, "a") as f:
        f.write(log_line)


def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "lxml")

    table = soup.find("table", class_= "wikitable")
    headers = [th.text.strip() for th in table.find_all("th")]
    rows = []
    for tr in table.find_all("tr")[1:11]:
        cells = tr.find_all("td")
        row = [cell.text.strip() for cell in cells]
        rows.append(row)
    df = pd.DataFrame(rows, columns=headers)

    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    df_csv = pd.read_csv(csv_path)
    dictionary = df_csv.set_index("Currency").to_dict()["Rate"]
    
    df['MC_GBP_Billion'] = [np.round(float(x)*dictionary['GBP'],2) for x in df['Market cap(US$ billion)']]
    df['MC_EUR_Billion'] = [np.round(float(x)*dictionary['EUR'],2) for x in df['Market cap(US$ billion)']]
    df['MC_INR_Billion'] = [np.round(float(x)*dictionary['INR'],2) for x in df['Market cap(US$ billion)']]
    
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    
    df.to_csv(output_path, index = False)


def load_to_db(df, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
   
    conn = create_engine(f"sqlite:///{database}")
    df.to_sql(table_name, con = conn, if_exists="replace", index=False)

    return conn


def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    return pd.read_sql(query_statement, con = sql_connection)



''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress("Preliminaries complete. Initiating ETL process")

df = extract(url, table_attribs)

log_progress("Data extraction complete. Initiating Transformation process")

df = transform(df, Exchange_rate_CSV_path)

log_progress("Data transformation complete. Initiating Loading process")

load_to_csv(df, csv_path_file)

log_progress("Data saved to CSV file")

conn = load_to_db(df, table_name)

log_progress("SQL Connection initiated")

log_progress("Data loaded to Database as a table, Executing queries")

print(run_query("SELECT * FROM Largest_banks", conn))
print(run_query("SELECT AVG(MC_GBP_Billion) FROM Largest_banks", conn))
print(run_query("SELECT * from Largest_banks LIMIT 5", conn))

log_progress("Process Complete")

log_progress("Server Connection closed")


