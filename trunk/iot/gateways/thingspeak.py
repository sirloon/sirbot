import time as modtime
import datetime
import httplib
import urllib

from base import IOTGateway, IOTGatewayException

import logging
LOG = logging.getLogger(__name__)


class ThingSpeakGateway(IOTGateway):

    def __init__(self,msg_class,api_key,field_mapping={},utc_shift=modtime.altzone,*args,**kwargs):
        super(ThingSpeakGateway,self).__init__(utc_shift,*args,**kwargs)
        self.msg_class = msg_class # acception message from class ...
        self.api_key = api_key
        # ThingSpeak has numeric fields, allow to map data field to custom field name
        self.fields_mapping = field_mapping
        self._server = "api.thingspeak.com:80"
        self._path = "/update"
        self._method = "POST"
        self._headers = {"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}

    def prepare(self,data,timestamp):
        pdata = {}
        for k,v in data.items():
            pdata[self.fields_mapping.get(k,k)] = v
        # common
        pdata['key'] = self.api_key
        pdata['created_at'] = timestamp
        LOG.debug("Mapped data: %s" % pdata)
        params = urllib.urlencode(pdata)
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
                raise IOTGatewayException("Failed to send data to ThingSpeak, got status:%s, reason:%s" % (response.status,response.reason))
        finally:
            conn.close()

