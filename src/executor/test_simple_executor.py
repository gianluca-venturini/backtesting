import pytest
from executor.simple_executor import SimpleExecutor
from datetime import datetime

def test_request_new_order():
    executor = SimpleExecutor()
    time = datetime(2019, 1, 1)
    executor.request_new_order(time, 'SYMBOL', 12, 'buy', 'stop', 'day', 10, None, True, 'id123')
    assert {
        'symbol': 'SYMBOL',
        'qty': 12,
        'side': 'buy',
        'type': 'stop',
        'time_in_force': 'day',
        'limit_price': 10,
        'stop_price': None,
        'extended_hours': True,
        'client_order_id': 'id123',
        'status': 'open',
        'created_at': time,
        'submitted_at': time,
        'filled_qty': 0
    } in executor.state['orders']
