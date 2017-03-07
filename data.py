# data.py

import os, os.path
import pandas as pd
from datetime import datetime, timedelta

from abc import ABCMeta, abstractmethod

from event import MarketEvent
from queue import Queue
import poloniex


class DataHandler(object):
    """
    DataHandler is an abstract base class providing an interface for
    all subsequent (inherited) data handlers (both live and historic).

    The goal of a (derived) DataHandler object is to output a generated
    set of bars (OLHCVI) for each symbol requested. 

    This will replicate how a live strategy would function as current
    market data would be sent "down the pipe". Thus a historic and live
    system will be treated identically by the rest of the backtesting suite.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars from the latest_symbol list,
        or fewer if less bars are available.
        """
        raise NotImplementedError("Should implement get_latest_bars()")

    @abstractmethod
    def update_bars(self):
        """
        Pushes the latest bar to the latest symbol structure
        for all symbols in the symbol list.
        """
        raise NotImplementedError("Should implement update_bars()")


        
class PoloniexDataHandler(DataHandler):
    """
    HistoricCSVDataHandler is designed to read CSV files for
    each requested symbol from disk and provide an interface
    to obtain the "latest" bar in a manner identical to a live
    trading interface. 
    """

    def __init__(self, markets, events, start=datetime.now()-timedelta(hours=24), end=datetime.now()):
        """
        Initialises the historic data handler by requesting
        the location of the CSV files and a list of symbols.

        It will be assumed that all files are of the form
        'symbol.csv', where symbol is a string in the list.

        Parameters:
        events - The Event Queue.
        csv_dir - Absolute directory path to the CSV files.
        symbol_list - A list of symbol strings.
        """
        self.events = events
        self.markets = markets

        self.market_data = {}
        self.market_snapshots = {}
        self.latest_market_data = {m:[] for m in markets}
        self.continue_backtest = True       

        self.get_all_market_data(start,end)
        
    def get_latest_market_data(self, market, N=1):
        """
        Returns the last N bars from the latest_symbol list,
        or N-k if less available.
        """
        try:
            snapshot_list = self.latest_market_data[market]
        except KeyError:
            print("That symbol is not available in the historical data set.")
        else:
            snapshot = snapshot_list[-N:]
            if len(snapshot) > 0:
                return snapshot[0]
            else:
                return None

    def update_market_data(self):
        """
        Pushes the latest bar to the latest_symbol_data structure
        for all symbols in the symbol list.
        """
        for m in self.markets:
            try:
                snapshot = self._get_new_snapshot(m)
            except StopIteration:
                self.continue_backtest = False
            else:
                if snapshot is not None:
                    self.latest_market_data[m].append(snapshot)
        self.events.put(MarketEvent())
        
    def _get_new_snapshot(self, market):
        """
        Returns the latest bar from the data feed as a tuple of 
        (sybmbol, datetime, open, low, high, close, volume).
        """
        return next(self.market_snapshots[market])[1]
    
    def get_all_market_data(self, start, end):
        
        for m in self.markets:
            self.market_data[m] = poloniex.get_trades(m, start, end)
            self.market_snapshots[m] = self.market_data[m].iterrows()
