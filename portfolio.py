import datetime
import numpy as np
import pandas as pd
from queue import Queue

from abc import ABCMeta, abstractmethod
from math import floor

from event import FillEvent, OrderEvent
from performance import create_sharpe_ratio, create_drawdowns

class Portfolio(object):
    """
    The Portfolio class handles the positions and market
    value of all instruments at a resolution of a "bar",
    i.e. secondly, minutely, 5-min, 30-min, 60 min or EOD.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def update_signal(self, event):
        """
        Acts on a SignalEvent to generate new orders 
        based on the portfolio logic.
        """
        raise NotImplementedError("Should implement update_signal()")

    @abstractmethod
    def update_fill(self, event):
        """
        Updates the portfolio current positions and holdings 
        from a FillEvent.
        """
        raise NotImplementedError("Should implement update_fill()")

class NaivePortfolio(Portfolio):
    """
    The NaivePortfolio object is designed to send orders to
    a brokerage object with a constant quantity size blindly,
    i.e. without any risk management or position sizing. It is
    used to test simpler strategies such as BuyAndHoldStrategy.
    """
    
    def __init__(self, market_data, events, start_date=None, initial_capital=100000.0):
        """
        Initialises the portfolio with bars and an event queue. 
        Also includes a starting datetime index and initial capital 
        (USD unless otherwise stated).

        Parameters:
        bars - The DataHandler object with current market data.
        events - The Event Queue object.
        start_date - The start date (bar) of the portfolio.
        initial_capital - The starting capital in USD.
        """
        self.market_data = market_data
        self.events = events
        self.markets = self.market_data.markets
        self.start_date = start_date
        if start_date is None:
            first_market = market_data.markets[0]
            self.start_date = market_data.market_data[first_market].index[0]
        self.initial_capital = initial_capital
        
        self.all_positions = self.construct_all_positions()
        self.current_positions = {s:0 for s in self.markets}

        self.all_holdings = self.construct_all_holdings()
        self.current_holdings = self.construct_current_holdings()
    
    def construct_all_positions(self):
        """
        Constructs the positions list using the start_date
        to determine when the time index will begin.
        """
        d = {s:0 for s in self.markets}
        d['datetime'] = self.start_date
        return [d]
             
    def construct_all_holdings(self):
        """
        Constructs the holdings list using the start_date
        to determine when the time index will begin.
        """
        d = {s:0 for s in self.markets}
        d['datetime'] = self.start_date
        d['cash'] = self.initial_capital
        d['commission'] = 0.0
        d['total'] = self.initial_capital
        return [d]
    
    # portfolio.py

    def construct_current_holdings(self):
        """
        This constructs the dictionary which will hold the instantaneous
        value of the portfolio across all symbols.
        """
        d = {s:0 for s in self.markets}
        d['cash'] = self.initial_capital
        d['commission'] = 0.0
        d['total'] = self.initial_capital
        return d

    def update_timeindex(self, event):
        """
        Adds a new record to the positions matrix for the current 
        market data bar. This reflects the PREVIOUS bar, i.e. all
        current market data at this stage is known (OLHCVI).

        Makes use of a MarketEvent from the events queue.
        """
        market_data = {}
        positions = {}
        holdings = {}
        holdings['cash'] = self.current_holdings['cash']
        holdings['total'] = self.current_holdings['cash']
        holdings['commission'] = self.current_holdings['commission']
        for market in self.markets:
            
            snapshot = self.market_data.get_latest_market_data(market, N=1)
            if snapshot is not None:
                positions[market] = self.current_positions[market]
                positions['datetime'] = snapshot.name
                holdings['datetime'] = snapshot.name
                market_value = positions[market] * snapshot.rate['mean']
                holdings['total'] += market_value
               
                
                
        # Append the current positions
        self.all_positions.append(positions)
        # Append the current holdings
        self.all_holdings.append(holdings)

    def update_positions_from_fill(self, fill):
        """
        Takes a FilltEvent object and updates the position matrix
        to reflect the new position.

        Parameters:
        fill - The FillEvent object to update the positions with.
        """
        # Check whether the fill is a buy or sell
        fill_dir = 0
        if fill.direction == 'BUY':
            fill_dir = 1
        if fill.direction == 'SELL':
            fill_dir = -1

        # Update positions list with new quantities
        self.current_positions[fill.market] += fill_dir*fill.quantity
        
    def update_holdings_from_fill(self, fill):
        """
        Takes a FillEvent object and updates the holdings matrix
        to reflect the holdings value.

        Parameters:
        fill - The FillEvent object to update the holdings with.
        """
        # Check whether the fill is a buy or sell
        fill_dir = 0
        if fill.direction == 'BUY':
            fill_dir = 1
        if fill.direction == 'SELL':
            fill_dir = -1

        # Update holdings list with new quantities
        cost = fill_dir * fill.price * fill.quantity
        self.current_holdings[fill.market] += cost
        self.current_holdings['commission'] += fill.commission
        self.current_holdings['cash'] -= (cost + fill.commission)
        self.current_holdings['total'] -= (cost + fill.commission)
        
    def update_fill(self, event):
        """
        Updates the portfolio current positions and holdings 
        from a FillEvent.
        """
        if event.type == 'FILL':
            self.update_positions_from_fill(event)
            self.update_holdings_from_fill(event)
            
    def generate_naive_order(self, signal):
        """
        Simply transacts an OrderEvent object as a constant quantity
        sizing of the signal object, without risk management or
        position sizing considerations.

        Parameters:
        signal - The SignalEvent signal information.
        """
        order = None

        market = signal.market
        strength = signal.strength
        price = signal.market_snapshot.rate['mean']
        
        

        mkt_quantity = floor(100 * strength)
        cur_quantity = self.current_positions[market]
        order_quantity = mkt_quantity
        order_type = 'MKT'
           
        print(signal.signal_type, cur_quantity)
        if signal.signal_type == 'LONG' and cur_quantity == 0:
            direction = 'BUY'
            return OrderEvent(market, direction, order_quantity, price)
        if signal.signal_type == 'SHORT' and cur_quantity == 0:
            direction = 'SELL'
            return OrderEvent(market, direction, order_quantity, price)
        if signal.signal_type == 'SHORT' and cur_quantity > 0:
            direction = 'SELL'
            order_quantity = 2*abs(cur_quantity)
            return OrderEvent(market, direction, order_quantity, price)
        if signal.signal_type == 'LONG' and cur_quantity < 0:
            direction = 'BUY'
            order_quantity = 2*abs(cur_quantity)
            return OrderEvent(market, direction, order_quantity, price)
            
    
    
    def update_signal(self, event):
        """
        Acts on a SignalEvent to generate new orders 
        based on the portfolio logic.
        """
        if event.type == 'SIGNAL':
            order_event = self.generate_naive_order(event)
            self.events.put(order_event)
            
    def create_equity_curve_dataframe(self):
        """
        Creates a pandas DataFrame from the all_holdings
        list of dictionaries.
        """
        curve = pd.DataFrame(self.all_holdings)
        curve.set_index('datetime', inplace=True)
        curve['returns'] = curve['total'].pct_change()
        curve['equity_curve'] = (1.0+curve['returns']).cumprod()
        self.equity_curve = curve
        
    def output_summary_stats(self):
        """
        Creates a list of summary statistics for the portfolio such
        as Sharpe Ratio and drawdown information.
        """
        total_return = self.equity_curve['equity_curve'][-1]
        returns = self.equity_curve['returns']
        pnl = self.equity_curve['equity_curve']

        sharpe_ratio = create_sharpe_ratio(returns)
        max_dd, dd_duration = create_drawdowns(pnl)

        stats = [("Total Return", "%0.2f%%" % ((total_return - 1.0) * 100.0)),
                 ("Sharpe Ratio", "%0.2f" % sharpe_ratio),
                 ("Max Drawdown", "%0.2f%%" % (max_dd * 100.0)),
                 ("Drawdown Duration", "%d" % dd_duration)]
        return stats

