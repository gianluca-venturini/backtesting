def get_values_at_timestamp(df, timestamp_ms):
    return {
        level: {symbol:df[level].to_dict()[symbol][timestamp_ms] for symbol in df[level].to_dict()}
        for level in ('o', 'c', 'l', 'h')
    }