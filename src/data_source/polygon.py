import requests
from datetime import datetime

MAX_PAGES = 1000

def _format_datetime(dt):
    timestamp_ms = int(dt.timestamp() * 1000)
    return timestamp_ms

def _format_datetime_log(dt, interval):
    if interval in ('day'):
        return dt.strftime('%Y-%m-%d')
    if interval in ('minute'):
        return dt.strftime('%Y-%m-%dT%H:%M:%S')
    raise Exception('Interval not supported {}'.format(interval))

def get_aggregate(symbol, interval, start, end, api_key):
    assert interval in {'day', 'minute'}
    assert isinstance(start, datetime)
    assert isinstance(end, datetime)

    results = []
    time_intervals = set()
    finished = False
    page = 0
    while (not finished):
        print('Fetch from {} to {}'.format(_format_datetime_log(start, interval), _format_datetime_log(end, interval)))
        if page > MAX_PAGES:
            raise Exception('Too many pages downloaded: {}'.format('page'))
        response = requests.get('https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/{interval}/{start}/{end}?apiKey={api_key}'.format(**{
            'symbol': symbol,
            'interval': interval,
            'start': _format_datetime(start),
            'end': _format_datetime(end),
            'api_key': api_key
        }))
        reponse_dict = response.json()
        finished = True
        if reponse_dict['results'] is None:
            break
        for result in reponse_dict['results']:
            timestamp = result['t']
            # Deduplicate results
            if timestamp not in time_intervals:
                finished = False
                time_intervals.add(timestamp)
                results.append(result)
        if len(reponse_dict['results']) > 0:
            end = datetime.fromtimestamp(reponse_dict['results'][0]['t'] / 1000.0)
        page += 1
    return results
