from datetime import datetime
from pytz import timezone, utc
from collections import defaultdict
from pandas import DataFrame
import matplotlib.pyplot as plt

# Orders should follow this format
# {
#   "id": "904837e3-3b76-47ec-b432-046db621571b",
#   "client_order_id": "904837e3-3b76-47ec-b432-046db621571b",
#   "created_at": "2018-10-05T05:48:59Z",
#   "updated_at": "2018-10-05T05:48:59Z",
#   "submitted_at": "2018-10-05T05:48:59Z",
#   "filled_at": "2018-10-05T05:48:59Z",
#   "expired_at": "2018-10-05T05:48:59Z",
#   "canceled_at": "2018-10-05T05:48:59Z",
#   "failed_at": "2018-10-05T05:48:59Z",
#   "asset_id": "904837e3-3b76-47ec-b432-046db621571b",
#   "symbol": "AAPL",
#   "asset_class": "us_equity",
#   "qty": "15",
#   "filled_qty": "0",
#   "type": "market",
#   "side": "buy",
#   "time_in_force": "day",
#   "limit_price": "107.00",
#   "stop_price": "106.00",
#   "filled_avg_price": "106.00",
#   "status": "accepted",
#   "extended_hours": false
# }
state = {
    # All orders
    'orders': [],
    # Initial cash
    'cash': 0,
    'portfolio': defaultdict(lambda: 0)
}

def request_new_order(time, symbol, qty, side, type, time_in_force, limit_price, stop_price, extended_hours, client_order_id):
    assert side in ('buy', 'sell')
    assert type in ('market', 'limit', 'stop', 'stop_limit')
    assert time_in_force in ('day', 'gtc', 'opg', 'cls', 'ioc', 'fok')

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
        now=time.astimezone(timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S %Z%z")
    ))

    state['orders'].append(order_dict)


def check_order_execution(time, row):
    low = float(row['l'])
    high = float(row['h'])
    for order in state['orders']:
        if order['status'] == 'open':
            if order['type'] == 'market':
                if order['side'] == 'buy':
                    execute_order(time, order, high)
                if order['side'] == 'sell':
                    execute_order(time, order, low)
            if order['type'] == 'limit':
                if order['side'] == 'buy' and order['limit_price'] > low:
                    execute_order(time, order, order['limit_price'])
                if order['side'] == 'sell' and order['limit_price'] < high:
                    execute_order(time, order, order['limit_price'])


def execute_order(time, order, price):
    print('Execute order {client_order_id} at {price}'.format(**order, price=price))
    order['status'] = 'filled'
    order['filled_avg_price'] = price
    order["filled_at"] = time
    order["filled_quantity"] = order["qty"]

    if order['side'] == 'buy':
       state['cash'] -=  price * order["qty"]
       state['portfolio'][order['symbol']] += order["qty"]
    if order['side'] == 'sell':
       state['cash'] +=  price * order["qty"]
       state['portfolio'][order['symbol']] -= order["qty"]

    print('Cash remaining {cash}'.format(**state))

    if state['cash'] < 0:
        raise Exception('Cash is below zero')
    if state['portfolio'][order['symbol']] < 0:
        raise Exception('Symbol {symbol} is below zero {current}'.format(**order, current=state['portfolio'][order['symbol']]))


def cancel_order(time, order, price):
    order['status'] = 'canceled'
    order["canceled_at"] = time


def expire_order(time, order, price):
    order['status'] = 'expired'
    order["expired_at"] = time


def portfolio_value(market):
    high = 0
    low = 0
    open = 0
    close = 0
    for symbol in state['portfolio']:
        high += float(market['h']) * state['portfolio'][symbol]
        low += float(market['l']) * state['portfolio'][symbol]
        open += float(market['o']) * state['portfolio'][symbol]
        close += float(market['c']) * state['portfolio'][symbol]
    cash = state['cash']
    return dict(high=high + cash, low=low + cash, open=open + cash, close=close + cash, cash=cash)


def execute_strategy(strategy, data, start, end, initial_cash=100000):
    assert start.tzinfo is not None, 'The start date should be timezone aware'
    assert end.tzinfo is not None, 'The end date should be timezone aware'

    state['cash'] = initial_cash

    portfolio_data = []

    for index, row in data.iterrows():
        utc_t = datetime.fromtimestamp(row['t'] / 1000, utc)

        check_order_execution(utc_t, row)

        strategy(lambda *args, **kw: request_new_order(utc_t, *args, **kw), row, state['cash'])

        today_portfolio_value = portfolio_value(row)
        today_portfolio_value['t'] = utc_t
        portfolio_data.append(today_portfolio_value)

    portfolio_data_frame = DataFrame(portfolio_data)

    print(portfolio_data_frame)
    ax = plt.gca()
    portfolio_data_frame.plot(kind='line', x='t', y='low', color='blue', ax=ax)
    plt.show()

    return portfolio_data_frame
