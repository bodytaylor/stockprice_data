import base64
import datetime
import os

import yfinance as yf
import pandas_gbq

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "credential/stockprice-collection-5188c972b4dd.json"

# Get data from yahoo finance
ticker = "^SET.BK"
data = yf.download(
    tickers = ticker,
    period = "MAX",
    end = datetime.date.today(),
    interval="1d"
    )
# reset index and give it a name
data.reset_index(inplace=True)
data = data.rename(columns = {'index':'date'})
data = data.drop('Adj Close' , axis = 1)

def upload_to_bigquary():
    pandas_gbq.to_gbq(
        data,
        destination_table = "stockprice_th.SET_index",
        project_id = "stockprice-collection",
        if_exists="append"
    )

upload_to_bigquary()