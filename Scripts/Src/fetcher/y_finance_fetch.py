import yfinance as yf
import time


def data_staging(data, Pk_name = "Us"):
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

    Potential errors
    ----------------
    401 or 429 : Unauthorized or too many requests 
    404 : Not found, Ticker data delisted or not present in yfinance data

    Future enhancemts
    ----------------
    Dynamic - .info.get("key") data -- fetched from yahoo not hard coded

    Fundamental metrics: 
    -------------------
        tickers.tickers[ticker].info.get('trailingPE'),
        tickers.tickers[ticker].info.get('forwardPE'),
        tickers.tickers[ticker].info.get('priceToBook'),
        tickers.tickers[ticker].info.get('priceToSalesTrailing12Months'),
        tickers.tickers[ticker].info.get('pegRatio'),
        tickers.tickers[ticker].info.get('profitMargins'),
        tickers.tickers[ticker].info.get('returnOnEquity'),
        tickers.tickers[ticker].info.get('returnOnAssets'),
        tickers.tickers[ticker].info.get('revenueGrowth'),
        tickers.tickers[ticker].info.get('earningsQuarterlyGrowth'),
        tickers.tickers[ticker].info.get('dividendYield') or info.get('trailingAnnualDividendYield'),
        tickers.tickers[ticker]info.get('debtToEquity'),
        tickers.tickers[ticker].info.get('marketCap'),
        tickers.tickers[ticker].info.get('operatingCashflow'),
        tickers.tickers[ticker].info.get('freeCashflow'),

    Furture Work 
    ------------
    Make it Dynamic - user sends the columns to be fetched
    '''
    staged_data = []
    count = 1
    for i in data:

        tickers = yf.Tickers(i)
        for j, ticker in enumerate(i, start= count):
            attempt = 0
            while attempt < 2:
                try:

                    staged_data.append((
                        ticker, 
                        tickers.tickers[ticker].info.get("longName", "Unknown"),
                        tickers.tickers[ticker].info.get("sector", "Unknown"),
                        tickers.tickers[ticker].info.get("country", "Unknown"),
                        tickers.tickers[ticker].info.get("industry", "Unknown"),
                        tickers.tickers[ticker].info.get("longBusinessSummary", "No summary available")))
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





