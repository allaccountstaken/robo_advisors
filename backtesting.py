"""
Part of robo-advisor library for portfolio management of market tradable securities,
imports to help with portfolio construction, optimization, allocation,and rebalancing,
taking into account individual risk tolerance and preferred choices of investment vehicles.
"""


# Help fetch historical market from external data providers, i.e. Quandl
class MarketDataSourse(object):
    def __init__(self, symbol, tick_event_handler=None, start='', end=''):
        self.market_data = MarketData()
        self.symbol = symbol
        self.ticker_event_handler = tick_event_handler
        self.start, self.end = start, end
        self.df = None

    def fetch_historical_prices(self):
        import quandl

        #Update your API key here...
        QUANDL_API_KEY = 'xxx'
        quandl.ApiConfig.api_key = QUANDL_API_KEY
        df = quandl.get(self.symbol, start_data=self.start, end_date=self.end)
        return df

    def run(self):
        if self.df is None:
            self.df = self.fetch_historical_prices()

        total_ticks = len(self.df)
        print('Processing total_ticks:', total_ticks)

        for timestamp, row, in self.df.iterrows():
            open_price = row['Open']
            close_price = row['Close']
            volume = row['Volume']

            print(timestamp.date(), 'TICK', self.symbol, 'open:', open_price, 'close:', close_price)

            tick_data = TickData(timestamp, self.symbol, open_price, close_price, volume)

            self.market_data.add_tick_data(tick_data)

            if self.tick_event_handler:
                self.tick_event_handler(self.market_data)

# Represents a single order sent by the Strategy to the market
class Order(object):
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
    def __init__(self, symbol=''):
        self.symbol = symbol
        self.buys = self.sells = self.net = 0
        self.rpnl = 0 # resently realized profits and losses for the symbol
        self.position_value = 0 # increased when securities are bought, decreased otherwise

    # When an order is filled, an account's position changes by running this method
    def on_position_event(self, is_buy, qty, price):
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

    #When position is open, the value of securities (pnl) is function of market movements
    def calculate_unrealized_pnl(self, price):
        if self.net == 0:
            return 0

        market_value = self.net * price
        upnl = self.position_value + market_value
        return upnl


# The Strategy class is abstract and serves as a base for all other trading strategies
from abs import abstractmethod

class Strategy():
    def __init__(self, send_order_event_handler):
        self.send_order_event_handler = send_order_event_handler

        @abstractmethod
        def on_tick_event(self, market_data):
            raise NotImplementedError('Method implementation is required')

        @abstractmethod
        def on_position_event(self, positions): # CHECK 'positions'?
            raise NotImplementedError('Method implementation is required')

        def send_market_order(self, symbol, qty, is_buy, timestamp):
            if self.send_order_event_handler:
                order = Order(timestamp, symbol, qty, is_buy, is_market_order=True, price=0)
                self.send_order_event_handler(order)

# Specific Strategy implementation - mean-reverting strategy
import pandas as pd

class MeanRevertingStrategy(Strategy):
    def __init__(self, symbol, trade_qty, send_order_event_handler=None,
        lookback_intervals=20, buy_threshold=-1.5, sell_threshold=1.5):
        super(MeanRevertingStrategy, self).__init__(send_order_event_handler)
        self.symbol = symbol
        self.trade_qty = trade_qty
        self.lookback_intervals = lookback_intervals
        self.buy_threshold = buy_threshold
        seld.sell_threshold = sell_threshold
        self.prices = pd.DataFrame()
        seld.is_long = self.is_short = False
# to be continued ...

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
