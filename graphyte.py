import requests
import pickle
import ConfigParser
import pandas as pd
import datetime
import logging
from urllib import urlencode

config = ConfigParser.SafeConfigParser()
config.read('graphyte.conf')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def request(params):
    # Parse options, config
    host = config.get('graphite', 'host')
    if config.has_option('graphite', 'sslcert'):
        cert = config.get('graphite', 'sslcert')
    else:
        cert = None
    params.update({ 'format': 'pickle' })
    options = urlencode(params, doseq=True)

    # Perform request
    try:
        if cert:
            reqData =  requests.get(host,
                                    params=options,
                                    cert=cert,
                                    verify=False)
        else:
            reqData =  requests.get(host,
                                    params=options,
                                    verify=False)
    except:
        raise

    graphiteData = pickle.loads(reqData.content)
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
        start = datetime.datetime.fromtimestamp(metric['start'])
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

def main(metrics):
    params = { 'target': metrics }
    g = request(params)
 

if __name__ == '__main__':
    from sys import argv
    #from arparse import ArgumentParser
    try:
        metrics = argv[1:]
    except IndexError as ie:
        raise IndexError('missing argument: metric')
    main(metrics)
