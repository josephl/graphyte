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
from datetime import datetime, time
from urllib import urlencode
from argparse import ArgumentParser
from ConfigParser import SafeConfigParser


def request(host, cert=None, **kwargs):
    """Perform request to Graphite Render API and return
    Datetimestamped Pandas DataFrame.
    host:   graphite host render URL, i.e. 'http://graphite.example.com/render'.
    cert:   ssl cert file.
    kwargs: render API URL query parameters"""
    # Extract non-graphite API options
    timeOptions = getTimeOptions(**kwargs)

    # Parse params
    options = parseRequestParams(**kwargs)

    if not host.endswith('/render'):
        host = host.rstrip('/') + '/render'

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

    df = getDataframe(graphiteData)

    # perform timeseries operations
    if 'resampleFreq' in timeOptions:
        print timeOptions
        if ('dayStart' in timeOptions and not (timeOptions['dayStart'] == 0 and
                timeOptions['dayEnd'] == 0)):
            print 'DAY RANGE'
            df = dayRange(df, timeOptions['dayStart'], timeOptions['dayEnd'],
                    timeOptions['resampleMethod'])
        df = resample(df, timeOptions['resampleFreq'],
                timeOptions['resampleMethod'])       
    elif ('dayStart' in timeOptions and not (timeOptions['dayStart'] == 0 and
            timeOptions['dayEnd'] == 0)):
        df = dayRange(df, timeOptions['dayStart'], timeOptions['dayEnd'],
                timeOptions['resampleMethod'])

    return df

def getDataframe(rawData, resample=None, how='sum'):
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

def plotData(df, fill='ffill', drop=False):
    """Convert data values to simple list of lists for jsonification, 
    plotting from pandas timeseries dataframes.
    returns column labels, value tuples."""
    values = []
    stats = []
    corr = correlations(df)
    while len(df.columns) > 0:
        series = df.pop(df.columns[0])
        timestamp = map(lambda t: int(t.to_datetime().strftime('%s')) * 1000,
                series.index.tolist())
        seriesDict = {
                'data': flotzip(timestamp, series.tolist()),
                'label': series.name
                }
        values.append(seriesDict)
        seriesStat = getStatObject(series)
        seriesStat.update({ 'corr': corr.pop(corr.columns[0]).values.tolist() })
        stats.append(seriesStat)
    # force diagonal to 1.0 (as NaN values are set to 0)
    for i in xrange(0, len(stats)):
        stats[i]['corr'][i] = 1.0
    return values, stats

def getStatObject(series):
    from math import isnan
    statObj = {}
    summ = series.sum()
    quartile = [
            series.quantile(0.0),
            series.quantile(0.25),
            series.quantile(0.5),
            series.quantile(0.75),
            series.quantile(1.0)
            ]
    mean = series.mean()
    var = series.var()
    if series.index.freq is not None:
        freq = series.index.freq.freqstr
    else:
        freq = ''
    # empty dataset
    if isnan(summ):
        return {
                'sum': 0.0,
                'quartile': [0.0, 0.0, 0.0, 0.0, 0.0],
                'mean': 0.0,
                'variance': 0.0,
                'freq': freq
            }
    return {
            'sum': summ,
            'quartile': quartile,
            'mean': mean,
            'variance': var,
            'freq': freq
        }

def correlations(df):
    return df.corr().fillna(0.0)

def flotzip(timestamp, serieslist):
    """For flot uncontinuous lines, requires a 'null' placeholder in place of
    a coordinate pair."""
    from math import isnan
    nulledSeries = zip(timestamp,
                       map(lambda x: None if x is None or isnan(x) else x,
                           serieslist))
    # trim leading null values
    try:
        i = 0
        while i < len(nulledSeries) and nulledSeries[i][1] == None:
            i += 1
        nulledSeries = nulledSeries[i:]
        print 'nulledSeries %d' % (len(nulledSeries))
    except:
        raise
    return nulledSeries

def getTimeOptions(**kwargs):
    """Extract non-graphite API options, return dict.
    Valid options: resampleFreq, resampleMethod, 
    dayStart, dayEnd."""
    timeOptions = {}
    if 'resampleFreq' in kwargs and kwargs['resampleFreq'] is not None:
        timeOptions.update({ 'resampleFreq': kwargs.pop('resampleFreq') })
        if 'resampleMethod' in kwargs:
            timeOptions.update(
                    { 'resampleMethod': kwargs.pop('resampleMethod') })
        else:
            timeOptions.update(
                    { 'resampleMethod': 'mean' })

    if 'dayStart' in kwargs and kwargs['dayStart'] is not None:
        timeOptions.update({
                'dayStart': int(kwargs.pop('dayStart')),
                'dayEnd': int(kwargs.pop('dayEnd'))
                })
    if 'resampleMethod' in kwargs and kwargs['resampleMethod'] is not None:
        timeOptions.update(
                { 'resampleMethod': kwargs.pop('resampleMethod') })
    else:
        timeOptions.update(
                { 'resampleMethod': 'mean' })
    return timeOptions

def resample(df, freq, method='mean'):
    return df.resample(freq, how=method)

def dayRange(df, dayStart, dayEnd, method='mean'):
    """dayStart, dayEnd are 0-23 hour values in the day."""
    start = time(hour=dayStart)
    end = time(hour=dayEnd)
    indexes = df.index.indexer_between_time(start, end)
    noneTuple = (None,) * len(df.columns)
    for i in xrange(len(df.index)):
        if i not in indexes:
            df.ix[i] = noneTuple
    return df

def parseRequestParams(**kwargs):
    graphiteArgs = { 'target': kwargs['target'] }
    if 'from' in kwargs and kwargs['from'] is not None:
        graphiteArgs.update({ 'from': kwargs['from'] })
    if 'until' in kwargs and kwargs['until'] is not None:
        graphiteArgs.update({ 'until': kwargs['until'] })
    graphiteArgs.update({ 'format': 'pickle' })
    return urlencode(graphiteArgs, doseq=True)

def mergeAnalytics(graphiteDF, analyticsDF):
    """Merge analytics data into graphite data, using graphite data index."""
    for col in analyticsDF.columns:
        trimmedADF = analyticsDF[col].ix[graphiteDF.index]
        graphiteDF[col] = pd.Series(trimmedADF.values.tolist(),
                                    index=graphiteDF.index)
    return graphiteDF

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
    parser.add_argument('target', metavar='METRIC', nargs='+',
            help='Graphite metric names')
    parser.add_argument('--from', dest='from',
            required=False)
    parser.add_argument('--until', dest='until',
            required=False)
    parser.add_argument('--resampleFreq', dest='resampleFreq',
            required=False)
    parser.add_argument('--resampleMethod', dest='resampleMethod',
            required=False)
    parser.add_argument('--dayStart', dest='dayStart', type=int,
            required=False)
    parser.add_argument('--dayEnd', dest='dayEnd', type=int,
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

    req = request(host, cert, **args.__dict__)
    import pdb; pdb.set_trace()
    #dayRange(req)
    reqData = plotData(req, 'ffill', True)
    import pdb; pdb.set_trace()
 

if __name__ == '__main__':
    main()
