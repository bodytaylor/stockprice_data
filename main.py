import base64
import datetime
import os

import yfinance as yf
import pandas_gbq

class Config:
    destination_table = os.environ.get('destination_table')
    project_id = os.environ.get('project_id')
    

def get_data(ticker):    
    data = yf.download(
        tickers = ticker,
        start = datetime.date.today(),
        end = datetime.date.today() + datetime.timedelta(1),
        interval="1d"
        )
    # reset index and give it a name
    data.reset_index(inplace=True)
    data = data.rename(columns = {'index':'date'})
    data = data.drop('Adj Close' , axis = 1)
    return data

print(get_data(ticker="^SET.BK"))

def upload_to_bigquary(event, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         event (dict): The dictionary with data specific to this type of event.
         context (google.cloud.functions.Context): The Cloud Functions event metadata.
    """
    pandas_gbq.to_gbq(
        dataframe = get_data(ticker="^SET.BK"),
        destination_table = Config.destination_table,
        project_id = Config.project_id,
        if_exists="append"
    )

upload_to_bigquary()