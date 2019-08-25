from math import isnan

def force_finite(n, default=0):
    if isnan(n):
        return default
    return n