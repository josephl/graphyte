#!/usr/bin/env python

# Graphyte

# A Graphite Render API handler in Python.
# Call `request` and specify Graphite render options as
# keyword arguments, such as target(s), from, until, etc.
# Returns Datetime-stamped Pandas DataFrames.

# Author: Joseph Lee <joseph@idealist.org>
# Copyright (c) 2013 Action Without Borders
# Developed at Idealist.org
# This software may be distributed under the MIT License
# See LICENSE for more information


import requests
import pickle
import pandas as pd
from datetime import datetime
from urllib import urlencode
from argparse import ArgumentParser
from ConfigParser import SafeConfigParser


def request(host, cert=None, **kwargs):
    """Perform request to Graphite Render API and return
    Datetimestamped Pandas DataFrame.
    host:   graphite host render URL, i.e. 'http://graphite.example.com/render'.
    cert:   ssl cert file.
    kwargs: render API URL query parameters"""
    # Parse params
    options = parseRequestParams(**kwargs)

    # Perform request
    try:
        if cert:
            reqData =  requests.get(
                    host, params=options, cert=cert, verify=False)
        else:
            reqData =  requests.get(
                    host, params=options, verify=False)
    except:
        raise

    try:
        graphiteData = pickle.loads(reqData.content)
    except:
        raise

    return getDataFrame(graphiteData)

def getDataFrame(rawData, resample=None, how='sum'):
    """Return pandas.DataFrame containing graphite data.
    resample: freq string for pandas dataframe."""
    # frequency is the least common multiple of steps
    freqs = map(lambda x: int(x['step']), rawData)
    freq = lcm([i for i in freqs])

    series_list = []
    for metric in rawData:
        step = '%ds' % (metric['step'])
        start = datetime.fromtimestamp(metric['start'])
        timestamps = pd.date_range(start=start,
                                   periods=len(metric['values']),
                                   freq=step)
        series = pd.DataFrame({ metric['name']: metric['values'] },
                                index=timestamps)
        series_list.append(series)

    df = pd.concat(series_list, join='outer', axis=1)

    # resample
    if resample is not None and resample >= freq:
        df = df.resample(resample, how=how)    
    elif len(set(freqs)) > 1:
        df = df.resample('%ds' % (max(freqs)), how=how)

    return df

def parseRequestParams(**kwargs):
    if kwargs.has_key('from') and kwargs['from'] is None:
        kwargs.pop('from')
    if kwargs.has_key('until') and kwargs['until'] is None:
        kwargs.pop('until')
    kwargs.update({ 'format': 'pickle' })

    return urlencode(kwargs, doseq=True)

def lcm(nums, mult=None):
    if len(nums) > 0:
        if mult is None:
            mult = nums.pop(-1)
        else:
            last = nums.pop(-1)
            mult *= last / gcd(last, mult)
        return lcm(nums, mult)
    else:
        return mult

def gcd(a, b):
    if b == 0:
        return a
    else:
        return gcd(b, a % b)

def main():
    parser = ArgumentParser()
    parser.add_argument('target',
                        metavar='METRIC',
                        nargs='+',
                        help='Graphite metric names')
    parser.add_argument('--from',
                        dest='from',
                        required=False)
    parser.add_argument('--until',
                        dest='until',
                        required=False)
    args = parser.parse_args()

    # Configuration
    config = SafeConfigParser()
    config.read('/opt/graphite/conf/graphyte.conf')
    host = config.get('graphite', 'host')
    if config.has_option('graphite', 'sslcert'):
        cert = config.get('graphite', 'sslcert')
    else:
        cert = None

    print request(host, cert, **args.__dict__)
 

if __name__ == '__main__':
    main()
