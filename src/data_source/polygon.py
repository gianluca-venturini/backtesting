import requests
from datetime import datetime
from pandas import DataFrame, HDFStore, concat, pivot
from util.cache_util import get_cached_dataframe, get_cached_dict
from urllib.parse import urlencode

MAX_PAGES = 1000
BASE_API_URL = 'https://api.polygon.io'

def _format_datetime(dt):
    timestamp_ms = int(dt.timestamp() * 1000)
    return timestamp_ms

def _format_datetime_log(dt, interval):
    if interval in ('day'):
        return dt.strftime('%Y-%m-%d')
    if interval in ('minute'):
        return dt.strftime('%Y-%m-%dT%H:%M:%S')
    raise Exception('Interval not supported {}'.format(interval))

def get_aggregate_symbol(symbol, interval, start, end, api_key):
    assert interval in {'day', 'minute'}
    assert isinstance(start, datetime)
    assert isinstance(end, datetime)
    assert start.tzinfo is not None, 'The start date should be timezone aware'
    assert end.tzinfo is not None, 'The end date should be timezone aware'

    results = []
    time_intervals = set()
    finished = False
    page = 0
    while (not finished):
        def get_page():
            print('Fetch {} from {} to {}'.format(symbol, _format_datetime_log(start, interval), _format_datetime_log(end, interval)))
            if page > MAX_PAGES:
                raise Exception('Too many pages downloaded: {}'.format('page'))
            response = requests.get('{api}/v2/aggs/ticker/{symbol}/range/1/{interval}/{start}/{end}?apiKey={api_key}'.format(**{
                'api': BASE_API_URL,
                'symbol': symbol,
                'interval': interval,
                'start': _format_datetime(start),
                'end': _format_datetime(end),
                'api_key': api_key
            }))
            return response.json()
        reponse_dict = get_cached_dict('get_aggregate_symbol', '{}_{}_{}_{}_{}'.format(symbol, interval, _format_datetime(start), _format_datetime(end), page), get_page)
        finished = True
        if reponse_dict['results'] is None:
            break
        for result in reponse_dict['results']:
            timestamp = result['t']
            # Deduplicate results
            if timestamp not in time_intervals:
                finished = False
                time_intervals.add(timestamp)
                results.append({
                    't': int(result['t']),
                    'o': float(result['o']),
                    'c': float(result['c']),
                    'h': float(result['h']),
                    'l': float(result['l']),
                })
        if len(reponse_dict['results']) > 0:
            end = datetime.fromtimestamp(reponse_dict['results'][0]['t'] / 1000.0)
        page += 1
    df = DataFrame(results)
    df['symbol'] = symbol
    return df

def get_tickers(type, market, api_key):
    tickers = set()
    page = 1
    print('Get tickers')
    while True:
        def get_page():
            qs = urlencode({key:value for key, value in {
                'type': type,
                'market': market,
                'page': page,
                'sort': 'ticker',
                'apiKey': api_key,
            }.items() if value is not None})
            response = requests.get('{api}/v2/reference/tickers?{qs}'.format(**{
                'api': BASE_API_URL,
                'qs': qs
            }))
            tickers_dict = response.json()
            assert tickers_dict['status'] == 'OK', 'Status is {}'.format(status)
            assert tickers_dict['page'] == page
            assert len(tickers_dict['tickers']) > 0
            return tickers_dict

        tickers_dict = get_cached_dict('get_tickers', '{version}_{type}_{market}_{page}'.format(version=2, type=type, market=market, page=page), get_page)
        for ticker in tickers_dict['tickers']:
            tickers.add(ticker['ticker'])
        print('Page {page}, number of tickers: {num_tickers}/{total_tickers}'.format(page=page, num_tickers=len(tickers), total_tickers=tickers_dict['count']))
        if len(tickers) >= tickers_dict['count']:
            break
        page += 1
    print('Total number of tickers: {num_tickers}'.format(num_tickers=len(tickers)))
    return tickers

def get_stocks_aggregate_data(type, market, interval, start, end, api_key):
    assert interval in {'day', 'minute'}
    assert isinstance(start, datetime)
    assert isinstance(end, datetime)
    assert start.tzinfo is not None, 'The start date should be timezone aware'
    assert end.tzinfo is not None, 'The end date should be timezone aware'

    symbols = get_tickers(type, market, api_key)
    dfs = []
    for index, symbol in enumerate(symbols):
        print('Get aggreagate data for symbol {} ({}/{})'.format(symbol, index + 1, len(symbols)))
        dfs.append(get_aggregate_symbol(symbol, interval, start, end, api_key))
    data_concat = concat(dfs, sort=False)
    # Creates one row per timestamp
    data_pivoted = pivot(data_concat, index='t', columns='symbol')
    return data_pivoted

def get_ticker_type(api_key):
    print('Get ticker types')
    response = requests.get('{api}/v2/reference/types?apiKey={api_key}'.format(**{
        'api': BASE_API_URL,
        'api_key': api_key
    }))
    tickers_dict = response.json()
    assert tickers_dict['status'] == 'OK'
    return tickers_dict['results']


