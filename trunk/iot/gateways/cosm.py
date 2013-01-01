import time as modtime
import datetime, httplib, urllib
import simplejson

from base import IOTGateway

import logging
LOG = logging.getLogger(__name__)


class CosmGateway(IOTGateway):

    def __init__(self,msg_class,api_key,feed_id,utc_shift=modtime.altzone,*args,**kwargs):
        super(CosmGateway,self).__init__(utc_shift,*args,**kwargs)
        self.msg_class = msg_class # acception message from class ...
        self.api_key = api_key
        self.feed_id = int(feed_id)
        self._server = "api.pachube.com"
        self._path = "/v2/feeds/%d" % self.feed_id
        self._method = "PUT"
        self._headers = {"X-PachubeApiKey": self.api_key}

    def prepare(self,data,timestamp):
        pdata = {
            "version" : "1.0.0",
            "datastreams" : []
            }
        for k,v in data.items():
            d = {"at" : timestamp, "id" : k ,"current_value" : v}
            pdata['datastreams'].append(d)
        # common
        LOG.debug("Mapped data: %s" % pdata)
        params = simplejson.dumps(pdata)
        LOG.debug("Prepared data: %s" % params)
        return params

    def send(self,data,timestamp=None):
        timestamp = timestamp or datetime.datetime.now()
        assert type(timestamp) == datetime.datetime, "timestamp isn't a datetime.datetime"
        timestamp = self.adjust_utc(timestamp).strftime("%Y-%m-%dT%H:%M:%SZ") # "2011-09-11T08:12:18Z"
        LOG.info("%s: @%s, sending %s" % (self.__class__.__name__,timestamp,data))
        params = self.prepare(data,timestamp)
        conn = httplib.HTTPConnection(self._server)
        try:
            conn.request(self._method, self._path, params, self._headers)
            response = conn.getresponse()
            content = response.read()
            LOG.info("%s, status:%s, reason:%s, content:%s" % (self.__class__.__name__,response.status, response.reason,response.read()))
            if response.status != 200:
                raise IOTGatewayException("Failed to send data to Cosm, got status:%s, reason:%s" % (response.status,response.reason))
        finally:
            conn.close()
