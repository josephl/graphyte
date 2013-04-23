import requests
import pickle
import ConfigParser
import calc
import pandas as pd
import datetime
import logging

config = ConfigParser.SafeConfigParser()
config.read('graphyte.conf')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Graphyte:
    def __init__(self, params):
        self.host = config.get('graphite', 'host')
        if config.has_option('graphite', 'sslcert'):
            self.cert = config.get('graphite', 'sslcert')
        else:
            self.cert = None
        self.params = params
        self.params.update({ 'format': 'pickle' })

    def request(self):
        logger.info('GRAPHITE REQUEST')
        from sys import exc_info
        try:
            if self.cert:
                reqData =  requests.get(self.host,
                                        params=self.params,
                                        cert=self.cert,
                                        verify=False)
            else:
                reqData =  requests.get(self.host,
                                        params=self.params,
                                        verify=False)
        except:
            errs = exc_info()
        rawData = pickle.loads(reqData.content)[0]
        # set values
        self.step = rawData['step']
        self.setData(rawData)        
        self.data = self.data.fillna(0.)

    def setData(self, rawData):
        times = pd.Series(map(lambda x: datetime.datetime.fromtimestamp(
                              x * rawData['step'] + rawData['start']),
                    range(0, (rawData['end'] - rawData['start']) /
                              rawData['step'])))
        self.data = pd.Series(rawData['values'], index=times)
        # to convert back to epoch seconds:
        # time.mktime(self.data['timestamp'][i].to_pydatetime().timetuple())

    def timeTable(self, timeSeries=None):
        if timeSeries is None:
            timeSeries = self.data
        from time import mktime
        # multiply epoch secs * 1000, js flot expects epoch ms
        table = zip(
            map(lambda t: 1000 * int(mktime(t.to_pydatetime().timetuple())),
                timeSeries.index.tolist()),
            timeSeries.tolist())
        return table

    def metric_name(self, dataObject):
        import re
        raw = dataObject['name']
        pattern = '(?:.*[(])*([^,()]*)(?:[,}].*)*'
        match = re.search(pattern, raw)
        if match:
            dataObject.update({ 'metric_name': match.group(1) })

    #def summarize(self, intervalLength=60):
    #    summary = []
    #    timeIndex = []
    #    interval = intervalLength / self.step;
    #    for s in xrange(0, self.data.size / interval):
    #        timeIndex.append(self.data.index[s * interval])
    #        summary.append(sum(self.data[s * interval : (s + 1) * interval]))
    #    ts = pd.TimeSeries(summary, index=timeIndex)
    #    return self.timeSlice(ts)

    def summarize(self, intervalLength=60):
        interval = pd.DateOffset(seconds=intervalLength)
        logger.info('Interval: %d' % (intervalLength))
        summary = self.data.asfreq(
                    freq='%ds' % (intervalLength)).replace('NaN', 0.)
        import pdb; pdb.set_trace()
        return self.timeDaily(summary,
            summary.keys()[0], summary.keys()[-1],
            12, 15)

    def timeDaily(self, tsd,
                  dateStart, dateEnd,
                  timeStart, timeEnd,
                  weekdays=False):
        """Use pandas.Period to return target periods within an interval.
        Args:
            tsd: Timeseries data (pandas.Timeseries).
            dateStart: beginning of date range in epoch seconds.
            dateEnd: end of date range in epoch seconds.
            timeStart: beginning of time to be measured daily.
            timeEnd: end of time to be measured daily.
        Return:
            Daily Timeseries object with sums within daily timespans.
        """
        dates = tsd.keys()

        prng = pd.period_range(dateStart, dateEnd, freq='D')
        import pdb; pdb.set_trace()

        #selectDates = filter(lambda d: d.hour > 12 and d.hour < 15, dates)
        return tsd[selectDates]


def main(metric):
    params = { 'target': metric }
    g = Graphyte(params)
    g.request()
 
if __name__ == '__main__':
    from sys import argv
    try:
        metric = argv[1]
    except IndexError as ie:
        raise IndexError('missing argument: metric')
    main(metric)
