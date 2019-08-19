import os
import requests
import pickle
from datetime import datetime
from pandas import DataFrame, HDFStore, concat, pivot

MAX_PAGES = 1000
BASE_API_URL = 'https://api.polygon.io'
CACHE_PATH = '/tmp/cache'

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
            response = requests.get('{api}/v2/reference/tickers?apiKey={api_key}&type={type}&market={market}&page={page}'.format(**{
                'api': BASE_API_URL,
                'api_key': api_key,
                'type': type,
                'market': market,
                'page': page,
            }))
            tickers_dict = response.json()
            assert tickers_dict['status'] == 'OK'
            assert tickers_dict['page'] == page
            assert len(tickers_dict['tickers']) > 0
            return tickers_dict

        tickers_dict = get_cached_dict('get_tickers', '{version}_{type}_{market}_{page}'.format(version=1, type=type, market=market, page=page), get_page)
        for ticker in tickers_dict['tickers']:
            tickers.add(ticker['ticker'])
        print('Page {page}, number of tickers: {num_tickers}/{total_tickers}'.format(page=page, num_tickers=len(tickers), total_tickers=tickers_dict['count']))
        if len(tickers) >= tickers_dict['count']:
            break
        page += 1
    print('Total number of tickers: {num_tickers}'.format(num_tickers=len(tickers)))
    return tickers

def get_stocks_aggregate_data(interval, start, end, api_key):
    assert interval in {'day', 'minute'}
    assert isinstance(start, datetime)
    assert isinstance(end, datetime)
    assert start.tzinfo is not None, 'The start date should be timezone aware'
    assert end.tzinfo is not None, 'The end date should be timezone aware'

    symbols = get_tickers('cs', 'stocks', api_key)
    dfs = []
    for symbol in symbols:
        print('Get aggreagate data for symbol {}'.format(symbol))
        dfs.append(get_aggregate_symbol(symbol, interval, start, end, api_key))
    data_concat = concat(dfs)
    # Creates one row per timestamp
    data_pivoted = pivot(data_concat, index='t', columns='symbol')
    return data_pivoted

def get_cached_dict(namespace, key, get_dict):
    ensure_cache_path_created()
    file_path = get_file_path(namespace, key, 'pickle')

    if os.path.exists(file_path):
        print('Load from cache {namespace} {key}'.format(namespace=namespace, key=key))
        with open(file_path, 'rb') as f:
            return pickle.load(f)

    d = get_dict()
    with open(file_path, 'wb') as f:
        pickle.dump(d, f)

    return d

def get_cached_dataframe(namespace, key, get_df):
    ensure_cache_path_created()
    file_path = get_file_path(namespace, ext='h5')

    with pd.HDFStore(file_path) as store:
        if key in store.keys():
            return store[key]
        else:
            df = get_df()
            store[key] = df

    return df

def ensure_cache_path_created():
    if not os.path.exists(CACHE_PATH):
        os.makedirs(CACHE_PATH)

def get_file_path(namespace, key=None, ext=None):
    return '{path}/{namespace}_{key}.{ext}'.format(
        path=CACHE_PATH,
        namespace=namespace,
        key=key or '',
        ext=ext,
    )


def cache_size():
    ensure_cache_path_created()
    return sum(
        os.path.getsize(os.path.join(CACHE_PATH, f))
        for f
        in os.listdir(CACHE_PATH)
        if os.path.isfile(os.path.join(CACHE_PATH, f))
    )
