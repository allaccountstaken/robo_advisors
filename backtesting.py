"""
Part of robo-advisor library for portfolio management of market tradable securities,
imports to help with portfolio construction, optimization, allocation,and rebalancing,
taking into account individual risk tolerance and preferred choices of investment vehicles.
"""

class TickData(object):
    """
    Stores a single unit of data.
    """
    def __init__(self, timestamp='', symbol='',
                 open_price=0, close_price=0, total_volume=0):
        self.symbol = symbol
        self.timestamp = timestamp
        self.open_price = open_price
        self.close_price = close_price
        self.total_volume = total_volume
        
class MarketData(object):
    """
    Used to store and retrieve prices referenced by various components. Essentially, it is a container
    for storing the last available tick data. Additionl `get` helper functions are included to provide easy
    reference to the required information.
    """
    def __init__(self):
        self.recent_ticks = dict() # indexed by symbol
        
    def add_tick_data(self, tick_data):
        self.recent_ticks[tick_data.symbol] = tick_data
        
    def get_open_price(self, symbol):
        return self.get_tick_data(symbol).open_price
    
    def get_close_price(self, symbol):
        return self.get_tick_data(symbol).close_price
    
    def get_tick_data(self, symbol):
        return self.recent_ticks.get(symbol, TickData())
    
    def get_timestamp(self, symbol):
        return self.recent_ticks[symbol].timestamp


class MarketDataSourse(object):
    """
    Used to help fetch historical market from external data providers, i.e. Quandl.
    Stores results in a dataframe
    """
    def __init__(self, symbol, tick_event_handler=None, start='', end=''):
        self.market_data = MarketData()
        self.symbol = symbol
        self.ticker_event_handler = tick_event_handler
        self.start, self.end = start, end
        self.df = None

    def fetch_historical_prices(self): # TBD: how to change providers?
        """
        Method specific to quandl and needs changes to use data from other providers, consider taking 
        providers' name as parameter input into the function and implement diffrent get methods
        depending on providers' support
        """
        
        import quandl
        
        QUANDL_API_KEY = 'xxx' # Use API key here...
        quandl.ApiConfig.api_key = QUANDL_API_KEY
        df = quandl.get(self.symbol, start_data=self.start, end_date=self.end)
        
        return df

    def run(self):
        """
        Used to simulate the streaming prices from our data provider during backtesting
        """
        
        if self.df is None:
            self.df = self.fetch_historical_prices() # check presence of market data

        total_ticks = len(self.df)
        print('Processing total_ticks:', total_ticks)

        for timestamp, row, in self.df.iterrows():
            open_price = row['Open']
            close_price = row['Close']
            volume = row['Volume']

            print(timestamp.date(), 'TICK', self.symbol, 
                  'open:', open_price, 
                  'close:', close_price)

            tick_data = TickData(timestamp, self.symbol, open_price, # each tick is transformed into instance of TickData
                                 close_price, volume)

            self.market_data.add_tick_data(tick_data) # ... and added to MarketData object

            if self.tick_event_handler:
                self.tick_event_handler(self.market_data)

class Order(object):
    """
    Represents a single order sent by the Strategy to the market
    """
    def __init__(self, timestamp, symbol, sty, is_buy, is_market_order, price=0):
        self.timestamp = timestamp
        self.symbol = symbol
        self.qty = qty
        self.price = price
        self.is_buy = is_buy
        self.is_market_order = is_market_order
        self.is_filled = False
        self.filled_price = 0
        self.filled_time = None
        self.filled_qty = 0


class Position(object):
    """
    Keeps track of current market position and account balance for a specific instrument (ticker)xs
    """
    def __init__(self, symbol=''):
        self.symbol = symbol
        self.buys = self.sells = self.net = 0
        self.rpnl = 0 # recently realized profits and losses for the symbol
        self.position_value = 0 # increased when securities are bought, decreased otherwise

    def on_position_event(self, is_buy, qty, price):
        """
        When an order is filled, an account's position changes by running this method
        """
        if is_buy:
            self.buys += qty
        else:
            self.sells += qty

        self.net = self.buys - self.sells
        changed_value = qty * price * (-1 if is_buy else 1)
        self.position_value += changed_value

        if self.net == 0:
            self.rpnl = self.position_value
            self.position_value = 0

    def calculate_unrealized_pnl(self, price):
        """
        When position is open, the value of securities (pnl) is function of market movements
        """
        if self.net == 0:
            return 0

        market_value = self.net * price
        upnl = self.position_value + market_value
        return upnl


# Consider repackaging into a separate file    
from abc import abstractmethod

class Strategy():
    """
    The Strategy class is abstract and serves as a base for all other trading strategies
    """
    def __init__(self, send_order_event_handler):
        self.send_order_event_handler = send_order_event_handler

        @abstractmethod
        def on_tick_event(self, market_data):
            """
            This method is called when new market tick data arrives. Child strategies would have to implement
            this abstract method with their own logic/behavior to act upon incoming market prices.
            """
            raise NotImplementedError('Method on_tick_event implementation is required')

        @abstractmethod
        def on_position_event(self, positions): # TBD: what is `positions` in this context?
            """
            This method is called whenever there are updates to the existig positions. Child strategies would have
            to define what they do upon incoming position updates.
            """
            raise NotImplementedError('Method on_position_event implementation is required')

        def send_market_order(self, symbol, qty, is_buy, timestamp):
            """
            This method is called by child strategies to route a market order to the broker. The handler of such an event 
            is stored in the constructor , where the actual implementation is done by the owner of this class and possibly
            interfacing directly with the broker's API.
            """
            if self.send_order_event_handler:
                order = Order(timestamp, symbol, qty, is_buy, is_market_order=True, price=0)
                self.send_order_event_handler(order)

                
                
                

# Specific Strategy implementation - mean-reverting strategy - consider repackaging into a separate file
import pandas as pd

class MeanRevertingStrategy(Strategy):
    """
    Requirements: if the current market price is less than the average price, the stock is considered attractive 
    for purchase, with the expectation that the price will rise. When the current market price is above the average price, 
    the market price is expected to fall. In other words, deviations from the average price are expected to revert to the
    average or mean, hence the name.
    """
    def __init__(self, symbol, trade_qty, 
                 send_order_event_handler=None, 
                 lookback_intervals=20, buy_threshold=-1.5, sell_threshold=1.5): # 3 params generate signals for the strategy
        
        super(MeanRevertingStrategy, self).__init__(send_order_event_handler)
        
        self.symbol = symbol
        self.trade_qty = trade_qty
        self.lookback_intervals = lookback_intervals
        self.buy_threshold = buy_threshold
        seld.sell_threshold = sell_threshold
        
        self.prices = pd.DataFrame()
        seld.is_long = self.is_short = False
        
        def on_position_event(self, positions): # Again, how `positions` are build?
            position = positions.get(self.symbol)
            
            self.is_long = position and position.net > 0
            self.is_short = position and position.net < 0
            
        
        def on_tick_event(self, market_data):
            self.store_prices(market_data)
            
            if len(self.prices) < self.lookback_intervals:
                return
            
            self.generate_signals_and_send_order(market_data)
            
        def store_prices(self, market_data):
            pass # TBC....
            
            
            

# Engine stores the symbol and number of units to trade
class BacktestEngine:
    def __init__(self, symbol, trade_qty, start='', end=''):
        self.symbol = symbol
        self.trade_qty = trade_qty
        self.market_data_source = MarketDataSourse(symbol,
            tick_event_handler=self.on_tick_event, start=start, end=end)
        self.strategy = None
        self.unfilled_orders = []
        self.positions = dict()
        self.df_rpnl = None
