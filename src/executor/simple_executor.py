import calendar
from datetime import datetime
from pytz import timezone, utc
from collections import defaultdict
from pandas import DataFrame
import matplotlib.pyplot as plt
from bisect import bisect_left, bisect_right
from util.dataframe_util import get_values_at_timestamp


class SimpleExecutor:
    # Orders should follow this format
    # {
    #   "id": "904837e3-3b76-47ec-b432-046db621571b",
    #   "client_order_id": "904837e3-3b76-47ec-b432-046db621571b",
    #   "created_at": datetime("2018-10-05T05:48:59Z"),
    #   "updated_at": datetime("2018-10-05T05:48:59Z"),
    #   "submitted_at": datetime("2018-10-05T05:48:59Z"),
    #   "filled_at": datetime("2018-10-05T05:48:59Z"),
    #   "expired_at": datetime("2018-10-05T05:48:59Z"),
    #   "canceled_at": datetime("2018-10-05T05:48:59Z"),
    #   "failed_at": datetime("2018-10-05T05:48:59Z"),
    #   "asset_id": "904837e3-3b76-47ec-b432-046db621571b",
    #   "symbol": "AAPL",
    #   "asset_class": "us_equity",
    #   "qty": 15,
    #   "filled_qty": 0,
    #   "type": "market",
    #   "side": "buy",
    #   "time_in_force": "day",
    #   "limit_price": 107.00,
    #   "stop_price": 106.00,
    #   "filled_avg_price": 106.00,
    #   "status": "accepted",
    #   "extended_hours": false
    # }
    def __init__(self):
        self.state = {
            # All orders
            'orders': [],
            # Initial cash
            'cash': 0,
            'portfolio': defaultdict(lambda: 0)
        }

    def request_new_order(self, time, symbol, qty, side, type, time_in_force, limit_price, stop_price, extended_hours, client_order_id):
        assert side in ('buy', 'sell')
        if side in ('sell'):
            raise NotImplementedError()
        assert type in ('market', 'limit', 'stop', 'stop_limit')
        if type in ('limit', 'stop', 'stop_limit'):
            raise NotImplementedError()
        assert time_in_force in ('day', 'gtc', 'opg', 'cls', 'ioc', 'fok')
        if time_in_force in ('opg', 'cls', 'ioc', 'fok'):
            raise NotImplementedError()

        order_dict = {
            'symbol': symbol,
            'qty': qty,
            'side': side,
            'type': type,
            'time_in_force': time_in_force,
            'limit_price': limit_price,
            'stop_price': stop_price,
            'extended_hours': extended_hours,
            'client_order_id': client_order_id,
            'status': 'open',
            'created_at': time,
            'submitted_at': time,
            'filled_qty': 0
        }
        print('{now} New order issued {symbol} {qty}'.format(
            **order_dict,
            now=time.astimezone(timezone('US/Eastern')
                                ).strftime("%Y-%m-%d %H:%M:%S %Z%z")
        ))

        self.state['orders'].append(order_dict)

    def check_orders_execution(self, time, interval_data):
        for order in self.state['orders']:
            self.check_order_execution(time, interval_data, order)

    def check_order_execution(self, time, interval_data, order):
        high = interval_data['h'][order['symbol']]
        low = interval_data['l'][order['symbol']]
        if order['status'] == 'open':
            elapsed = time - order['created_at']
            if order['time_in_force'] == 'day' and elapsed.days >= 1:
                self.expire_order(time, order)
                return
            if order['type'] == 'market':
                if order['side'] == 'buy':
                    self.execute_order(time, order, high)
                if order['side'] == 'sell':
                    self.execute_order(time, order, low)
            if order['type'] == 'limit':
                if order['side'] == 'buy' and order['limit_price'] > low:
                    self.execute_order(time, order, order['limit_price'])
                if order['side'] == 'sell' and order['limit_price'] < high:
                    self.execute_order(time, order, order['limit_price'])

    def execute_order(self, time, order, price):
        print('Execute order {client_order_id} at {price}'.format(
            **order, price=price))
        order['status'] = 'filled'
        order['filled_avg_price'] = price
        order['filled_at'] = time
        order['filled_qty'] = order['qty']

        if order['side'] == 'buy':
            self.state['cash'] -= price * order['qty']
            self.state['portfolio'][order['symbol']] += order['qty']
        if order['side'] == 'sell':
            self.state['cash'] += price * order['qty']
            self.state['portfolio'][order['symbol']] -= order['qty']

        print('Cash remaining {cash}'.format(**self.state))

        if self.state['cash'] < 0:
            raise Exception('Cash is below zero')
        if self.state['portfolio'][order['symbol']] < 0:
            raise Exception('Symbol {symbol} is below zero {current}'.format(
                **order, current=self.state['portfolio'][order['symbol']]))

    def cancel_order(self, time, order):
        order['status'] = 'canceled'
        order['canceled_at'] = time

    def expire_order(self, time, order):
        order['status'] = 'expired'
        order['expired_at'] = time

    def portfolio_value(self, last_interval):
        high = 0
        low = 0
        open = 0
        close = 0
        for symbol in self.state['portfolio']:
            high += last_interval['h'][symbol] * \
                self.state['portfolio'][symbol]
            low += last_interval['l'][symbol] * self.state['portfolio'][symbol]
            open += last_interval['o'][symbol] * \
                self.state['portfolio'][symbol]
            close += last_interval['c'][symbol] * \
                self.state['portfolio'][symbol]
        cash = self.state['cash']
        return dict(high=high + cash, low=low + cash, open=open + cash, close=close + cash, cash=cash)

    def filter_data(self, data, start, end, include_end=True):
        assert start.tzinfo is not None, 'The start date should be timezone aware'
        assert end.tzinfo is not None, 'The end date should be timezone aware'

        timestamp_start = calendar.timegm(start.timetuple())
        timestamp_end = calendar.timegm(end.timetuple())
        # bisect_right returns the index after the found element
        index_start = bisect_right(data.index, timestamp_start * 1000) - 1
        index_end = bisect_left(data.index, timestamp_end * 1000)
        if include_end is False and data.index[index_end] / 1000 == timestamp_end:
            index_end -= 1
        assert index_start >= 0
        assert index_end >= -1
        return data[index_start:index_end + 1]

    def execute_strategy(self, strategy, data, start, end, initial_cash=100000, plot=False):
        assert start.tzinfo is not None, 'The start date should be timezone aware'
        assert end.tzinfo is not None, 'The end date should be timezone aware'

        self.state['cash'] = initial_cash

        portfolio_data = []

        # Timestamps in the interval
        timestamps_ms = self.filter_data(data, start, end).index

        for timestamp_ms in timestamps_ms:
            utc_t = datetime.fromtimestamp(timestamp_ms / 1000, utc)
            filtered_data = self.filter_data(data, start, utc_t, False)

            latest_interval = get_values_at_timestamp(data, timestamp_ms)
            assert latest_interval is not None

            self.check_orders_execution(utc_t, latest_interval)

            if len(filtered_data) > 0:
                strategy(utc_t, lambda *args, **kw: self.request_new_order(utc_t,
                                                                           *args, **kw), filtered_data, self.state['cash'])

            today_portfolio_value = self.portfolio_value(latest_interval)
            today_portfolio_value['t'] = utc_t
            portfolio_data.append(today_portfolio_value)
        print(timestamps_ms)

        portfolio_data_frame = DataFrame(portfolio_data)

        if plot:
            ax = plt.gca()
            portfolio_data_frame.plot(
                kind='line', x='t', y='low', color='blue', ax=ax)
            plt.show()

        return portfolio_data_frame
