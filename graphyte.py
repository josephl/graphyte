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
    from sys import exc_info
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
        errs = exc_info()

    rawData = pickle.loads(reqData.content)
    # groupby(lambda x: x.hour)
    return getDataFrame(rawData)

def getDataFrame(rawData):
    start = datetime.datetime.fromtimestamp(int(rawData[0]['start']))
    end = datetime.datetime.fromtimestamp(int(rawData[0]['end']))
    freq = '%ds' % (rawData[0]['step'])
    timeindex = pd.DatetimeIndex(start=start,
                                 end=end,
                                 freq=freq)[:-1]
    values = {}
    for metric in rawData:
        values.update({ metric['name']: metric['values']})
    
    df = pd.DataFrame(values, index=timeindex)
    return df

def main(metrics):
    params = { 'target': metrics }
    g = request(params)
 

if __name__ == '__main__':
    from sys import argv
    try:
        metrics = argv[1:]
    except IndexError as ie:
        raise IndexError('missing argument: metric')
    main(metrics)
