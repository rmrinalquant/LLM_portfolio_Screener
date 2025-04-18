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

def root_data(data,cursor):
    root_query = "Insert into MetaData_US_companies (Ticker, Company, Sector, Country, Industry, BusinessSummary) VALUES %s ON CONFLICT (Ticker) DO NOTHING"
    small_batch_job(data, root_query, cursor)
    
def fundamental_data(data,cursor):
    cursor.execute("Select Stock_Id,Ticker from MetaData_US_companies")
    query = "Insert into Fundamental_Metrics ( Stock_Id,trailing_pe,forward_pe,\
                    price_to_book,price_to_sales,peg_ratio,profit_margin,return_on_equity,return_on_assets,revenue_growth,\
                        eps_growth,dividend_yield,debt_to_equity,market_cap,operating_cash_flow,free_cash_flow) VALUES %s ON CONFLICT (Stock_Id) DO NOTHING"
    _data = cursor.fetchall()
    chunk = general.batch(data, 100)
    pair = {ticker: _id for _id, ticker in _data}
    staged_data = y_finance_fetch.data_staging(chunk, pair)
    insert_data.insert_data(staged_data, query, cursor) 
    print("Inserted data:",staged_data)


def main():
    
    conn = connection.get_connection(path)
    cursor = conn.cursor()
    
    #Schema.drop_table(cursor)
    #Schema.create_table(cursor)
    data = pd.read_csv(data_path('S&p_500.csv'))['Symbol'].to_list()
    fundamental_data(data,cursor)
    
    root_query = "Insert into MetaData_US_companies (Ticker, Company, Sector, Country, Industry, BusinessSummary) VALUES %s ON CONFLICT (Ticker) DO NOTHING"    
    #half = len(data)//2      # Send the data in batches hiting rate limit -- send half and other half after 15 mins -- input manually in the job function 

    #job(data[2000:4000], root_query, cursor)
    
    conn.commit()
    conn.close()
    cursor.close()

        

if __name__ == "__main__":
    main()
    
