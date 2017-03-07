import json
import time
import requests
import pandas as pd


def poloniex_api(command, args={}):
    url = 'https://poloniex.com/public?command='+command
    for arg, value in args.items():
        url += '&{}={}'.format(arg,value)
    return json.loads(requests.get(url).content.decode('utf-8'))

def get_trades(currency_pair, start, end):

    trades_df = pd.DataFrame()

    need_to_fetch = lambda t: len(t) == 0 or t.tradeID['count'].sum() % 50000 == 0
    while need_to_fetch(trades_df):

        print(start, end)
        new_trades = pd.DataFrame(
            poloniex_api('returnTradeHistory', {
                'currencyPair': currency_pair,
                'start': unix_time(start),
                'end': unix_time(end)
            }))

        if new_trades.empty:
            break

        trades = format_dtypes(new_trades)
        end = trades.index.min() # reset the end 

        # group trades by hour
        trades['hour'] = trades.index.to_period('1H')
        hourly_stats = trades.groupby('hour').describe().unstack()

        # account for overlap in period indes
        if not trades_df.empty and trades_df.index.min() == hourly_stats.index.max():
            row1 = trades_df.ix[trades_df.index.min()]
            row2 = hourly_stats.ix[trades_df.index.min()]
            trade_count = (row1.tradeID['count'] + row2.tradeID['count'])
            new_row = (
                ((row1 * row1.tradeID['count']) + (row2 * row2.tradeID['count'])) /  trade_count 
            )

            for col in new_row.index.levels[0]:
                new_row[col]['count'] = trade_count

            trades_df.ix[trades_df.index.min()] = new_row

            hourly_stats = hourly_stats[hourly_stats.index != trades_df.index.min()]

        trades_df = pd.concat([trades_df, hourly_stats])

        time.sleep(0.5)

    trades_df = trades_df.sort_index()
    trades_df['market'] = currency_pair
    print('acquired {} total trades'.format(trades_df.tradeID['count'].sum()))

    return trades_df

def unix_time(dt):
    return int(time.mktime(dt.timetuple()))

def format_dtypes(df):
    trades = df.copy()
    for col in ['amount','rate','total']:
        trades[col] = trades[col].astype(float, raise_on_error=False)
    trades['date'] = pd.to_datetime(trades.date)
    trades = trades.set_index('date')
    trades['type'] = trades.type.str.contains('buy') * 1

    return trades