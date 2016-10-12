import json
import requests
import pandas as pd
import time
from datetime import datetime, timedelta


def poloniex_api(command, args={}):
    url = 'https://poloniex.com/public?command='+command
    for arg, value in args.items():
        url += '&{}={}'.format(arg,value)
    return json.loads(requests.get(url).content.decode('utf-8'))

def unix_time(dt):
    return int(time.mktime(dt.timetuple()))

def format_dtypes(df):
    trades = df.copy()
    for col in ['amount','rate','total']:
        trades[col] = trades[col].astype(float, raise_on_error=False)
    trades['date'] = pd.to_datetime(trades.date)
    return trades
        
def get_trades(currency_pair, start, end, save_file=False, file_path='data/'):
    
    date_fmt = '%Y-%m-%dT%H:%M:%S'
    file_name = file_path+'Trades|{}|{}|{}.csv'.format(
        currency_pair, start.strftime(date_fmt), end.strftime(date_fmt))
    
    trades = pd.DataFrame()
    total_trades = len(trades)
    need_to_fetch = lambda t: len(t) == 0 or len(t) % 50000 == 0
    while need_to_fetch(trades):
        
        new_trades = pd.DataFrame(
            poloniex_api('returnTradeHistory', {
                'currencyPair': currency_pair,
                'start': unix_time(start),
                'end': unix_time(end if len(trades) == 0 
                                 else datetime.utcfromtimestamp(
                            trades.date.tail(1).values[0].tolist()/1e9))
            }))
        
        if new_trades.empty:
            break
        
        new_trades = format_dtypes(new_trades)
        
        if save_file:
            with open(file_name, 'a') as f:
                new_trades.to_csv(f, index=False)
                trades = new_trades
        else:
            trades = pd.concat([trades, new_trades])
        
        total_trades += len(new_trades)
        
        
        time.sleep(2)
    
    print('acquired {} total trades'.format(total_trades))
    
    if save_file:
        return file_name
    
    return trades 