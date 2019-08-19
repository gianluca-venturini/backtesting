import os
import pickle

CACHE_PATH = '/tmp/cache'


def ensure_cache_path_created():
    if not os.path.exists(CACHE_PATH):
        os.makedirs(CACHE_PATH)


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
