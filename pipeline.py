from Scripts.Src.Utils import general
from Scripts.Src.inserter import insert_data
from Scripts.Src.db import connection
from Scripts.Src.db import Schema
import os
from Scripts.Src.fetcher import y_finance_fetch
import pandas as pd
import time

'''
Run by checking the data size usually it hit the rate limit around 3000 tickers at a time.
    - Divide it into two halfs and run the second after the first half is done with time lag of 15 mins

Important
---------
- Use small batch function to gather data for size < 1000

Feature enhancements
-------------------
    Rollback feature
    Use this approach for end will figure out  the optimized technique 
    Scheduling jobs
    Multithreading
'''

# Setting up base directory for connection
base_dir  = os.path.abspath(os.path.join(__file__,".."))
path = os.path.join(base_dir,'Config', '.env')

# Setting up path for loading data
def data_path(file_name):
    base_dir = os.path.dirname(__file__)
    data_path = os.path.join(base_dir,"Data","Raw_data", f'{file_name}')
    return data_path

def small_batch_job(data, query, cursor):
    data = general.batch(data, 100)
    staged_data = y_finance_fetch.data_staging(data)
    insert_data.insert_data(staged_data, query, cursor)
    print("Inserted data:",staged_data)

def large_bathc_job(data, query, cursor):
    data = general.batch(data, 500)
        
    for val in data:
        chunk = general.batch(val, 100)
        staged_data = y_finance_fetch.data_staging(chunk)
        insert_data.insert_data(staged_data, query, cursor)  
        print("Inserted data:",staged_data)
        print("Sleeping for 100 seconds")          
        time.sleep(2)
        
def main():
    
    conn = connection.get_connection(path)
    cursor = conn.cursor()
    
    #Schema.drop_table(cursor)
    Schema.create_table(cursor)

    root_query = "Insert into MetaData_US_companies (Ticker, Company, Sector, Country, Industry, BusinessSummary) VALUES %s ON CONFLICT (Ticker) DO NOTHING"
    fundamental_query = " Insert into Fundamental_Metrics (Ticker, Price, BookValue, PE, MarketCap, Dividend) VALUES %s ON CONFLICT (Ticker) DO NOTHING"
    data = pd.read_csv(data_path('S&p_500.csv'))['Symbol'].to_list()
    
    small_batch_job(data, root_query, cursor)
    #half = len(data)//2      # Send the data in batches hiting rate limit -- send half and other half after 15 mins -- input manually in the job function 

    #job(data[2000:4000], root_query, cursor)
    
    conn.commit()
    conn.close()
    cursor.close()

        

if __name__ == "__main__":
    main()
    
