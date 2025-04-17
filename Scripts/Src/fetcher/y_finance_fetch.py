import yfinance as yf
import time

def data_staging(data, pair = None , Pk_name = "Us"):
    '''
    Gather data from yfinance.Tickers.info (explicitily)
    Always initialize root table  and insert the data first , then follow up by other insertion.
    Work as Scnema is designed - Manual labour


    Parameters
    ----------
    cursor : cursor object of database connection
    data : list of tickers (list of list) - batch

    Returns
    -------
    Data: list of list
        # Note for the case of Foreign keys, map manually in pipeline script 
    
    Potential errors
    ----------------
    401 or 429 : Unauthorized or too many requests 
    404 : Not found, Ticker data delisted or not present in yfinance data

    Future enhancemts
    ----------------
    Dynamic - .info.get("key") data -- fetched from yahoo not hard coded - User input Dictionary

    MetaData: 
    ---------
        tickers.tickers[ticker].info.get("longName", "Unknown"),
        tickers.tickers[ticker].info.get("sector", "Unknown"),
        tickers.tickers[ticker].info.get("country", "Unknown"),
        tickers.tickers[ticker].info.get("industry", "Unknown"),
        tickers.tickers[ticker].info.get("longBusinessSummary", "No summary available")
    
    Fundamental metrics: 
    -------------------
        tickers.tickers[ticker].info.get('trailingPE','Null'),
        tickers.tickers[ticker].info.get('forwardPE','Null'),
        tickers.tickers[ticker].info.get('priceToBook','Null'),
        tickers.tickers[ticker].info.get('priceToSalesTrailing12Months','Null'),
        tickers.tickers[ticker].info.get('pegRatio','Null'),
        tickers.tickers[ticker].info.get('profitMargins','Null'),
        tickers.tickers[ticker].info.get('returnOnEquity','Null'),
        tickers.tickers[ticker].info.get('returnOnAssets','Null'),
        tickers.tickers[ticker].info.get('revenueGrowth','Null'),
        tickers.tickers[ticker].info.get('earningsQuarterlyGrowth','Null'),
        tickers.tickers[ticker].info.get('dividendYield') or info.get('trailingAnnualDividendYield', 'Null'),
        tickers.tickers[ticker]info.get('debtToEquity', 'Null'),
        tickers.tickers[ticker].info.get('marketCap', 'Null'),
        tickers.tickers[ticker].info.get('operatingCashflow', 'Null'),
        tickers.tickers[ticker].info.get('freeCashflow', 'Null'),

    '''
    staged_data = []
    count = 1
    for i in data:
        tickers = yf.Tickers(i)
        for j, ticker in enumerate(i, start= count):
            if pair is not None:
                _stock_id = pair[ticker]

            attempt = 0
            while attempt < 2:
                try:
                    staged_data.append((
                        _stock_id,
                        tickers.tickers[ticker].info.get('trailingPE', 0),
                        tickers.tickers[ticker].info.get('forwardPE', 0),
                        tickers.tickers[ticker].info.get('priceToBook', 0),
                        tickers.tickers[ticker].info.get('priceToSalesTrailing12Months', 0),
                        tickers.tickers[ticker].info.get('pegRatio', 0),
                        tickers.tickers[ticker].info.get('profitMargins', 0),
                        tickers.tickers[ticker].info.get('returnOnEquity', 0),
                        tickers.tickers[ticker].info.get('returnOnAssets', 0),
                        tickers.tickers[ticker].info.get('revenueGrowth', 0),
                        tickers.tickers[ticker].info.get('earningsQuarterlyGrowth', 0),
                        tickers.tickers[ticker].info.get('trailingAnnualDividendYield', 0),
                        tickers.tickers[ticker].info.get('debtToEquity', 0),
                        tickers.tickers[ticker].info.get('marketCap', 0),
                        tickers.tickers[ticker].info.get('operatingCashflow', 0),
                        tickers.tickers[ticker].info.get('freeCashflow', 0),

                        ))
                    break
                except Exception as e:
                    attempt += 1    
                    print(f"Error fetching {ticker}: {e} (attempt {attempt})")
                    if "401" in str(e) or "429" in str(e):
                        print("Time lag")
                        time.sleep(300)
                    else:
                        break
        print("Staging data:",staged_data)
        
    return staged_data





