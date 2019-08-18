import pytest
from executor.simple_executor import SimpleExecutor
from datetime import datetime
from pandas import DataFrame, concat, pivot, MultiIndex
from pytz import timezone, utc
import calendar
from util.dataframe_util import get_values_at_timestamp


def get_data():
    ts_ms1 = calendar.timegm(
        datetime(2019, 1, 1, tzinfo=utc).timetuple()) * 1000
    ts_ms2 = calendar.timegm(
        datetime(2019, 1, 2, tzinfo=utc).timetuple()) * 1000
    data_SYMBOL1 = DataFrame([
        {
            'o': 10,
            'c': 10,
            'l': 5,
            'h': 15,
            't': ts_ms1,
            'symbol': 'SYMBOL1',
        },
        {
            'o': 10,
            'c': 10,
            'l': 5,
            'h': 15,
            't': ts_ms2,
            'symbol': 'SYMBOL1',
        }
    ])

    data_SYMBOL2 = DataFrame([
        {
            'o': 10,
            'c': 10,
            'l': 5,
            'h': 15,
            't': ts_ms1,
            'symbol': 'SYMBOL2',
        },
        {
            'o': 10,
            'c': 10,
            'l': 5,
            'h': 15,
            't': ts_ms2,
            'symbol': 'SYMBOL2',
        }
    ])

    data_concat = concat([data_SYMBOL1, data_SYMBOL2])
    # Creates one row per timestamp
    data_pivoted = pivot(data_concat, index='t', columns='symbol')
    return data_pivoted


def test_request_new_order():
    executor = SimpleExecutor()
    time = datetime(2019, 1, 1)
    executor.request_new_order(
        time, 'SYMBOL', 12, 'buy', 'stop', 'day', 10, None, True, 'id123')
    assert len(executor.state['orders']) == 1
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


def test_execute_strategy_request_new_order():
    executor = SimpleExecutor()
    start = datetime(2019, 1, 1, tzinfo=utc)
    end = datetime(2019, 2, 1, tzinfo=utc)
    data = get_data()

    def mock_strategy(now, request_new_order, data, cash):
        last_ts_ms = data.index[-1]
        last_interval = get_values_at_timestamp(data, last_ts_ms)
        assert last_interval['o']['SYMBOL1'] == 10
        assert cash == 100
        # Makes sure data doesn't start before the start of the interval
        assert datetime.fromtimestamp(data.index[0] / 1000, utc) >= start
        # Makes sure data doesn't end after the end of the interval
        assert datetime.fromtimestamp(data.index[-1] / 1000, utc) <= end
        # Makes sure data doesn't contain anything in the future. The equal
        # is excluded because the ts of the data is floored with the interval starting ts
        assert datetime.fromtimestamp(data.index[-1] / 1000, utc) < now
        request_new_order('SYMBOL1', 12, 'buy', 'stop',
                          'day', 10, None, True, 'id123')

    assert len(executor.state['orders']) == 0
    executor.execute_strategy(
        mock_strategy,
        data,
        start,
        end,
        initial_cash=100)
    assert len(executor.state['orders']) == 1


def test_filter_data():
    executor = SimpleExecutor()
    start = datetime(2019, 1, 1, tzinfo=utc)
    end = datetime(2019, 1, 2, tzinfo=utc)
    data = get_data()

    assert len(executor.filter_data(data, start, end)) == 2
    assert len(executor.filter_data(data, start, end, include_end=False)) == 1
