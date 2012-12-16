import sys
import time as modtime
import datetime
import httplib
import urllib

from twisted.internet import defer

import config
import logging
LOG = logging.getLogger(__name__)


class IOTGateway(object):
    """Common interface"""

    def __init__(self,utc_shift=modtime.altzone,*args,**kwargs):
        self.utc_shift = utc_shift

    def send(self,data,timestamp=None):
        raise Exception("Not implemented")

    def adjust_utc(self,timestamp):
        return timestamp + datetime.timedelta(seconds=self.utc_shift)

    def prepare(self,data,timestamp):
        """Return formatted data according to gateway expectations"""
        raise Exception("Not implemented")

class IOTGatewayException(Exception):
    pass

#class HTTPConnexion(object):
#        headers = {"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
#            conn = httplib.HTTPConnection("api.thingspeak.com:80")
#                conn.request("POST", "/update", params, headers)



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
        LOG.debug("Prepared data: %s" % pdata)
        return params

    def send(self,data,timestamp=None):
        timestamp = timestamp or datetime.datetime.now()
        assert type(timestamp) == datetime.datetime, "timestamp isn't a datetime.datetime"
        timestamp = self.adjust_utc(timestamp).strftime("%Y-%m-%dT%H:%M:%SZ") # "2011-09-11T08:12:18Z"
        LOG.info("%s: @%s, sending %s" % (self.__class__.__name__,timestamp,data))
        params = self.prepare(data,timestamp)
        conn = httplib.HTTPConnection(self._server)
        conn.request(self._method, self._path, params, self._headers)
        response = conn.getresponse()
        content = response.read()
        LOG.info("%s, status:%s, reason:%s, content:%s" % (self.__class__.__name__,response.status, response.reason,response.read()))
        if response.status != 200:
            raise IOTGatewayException("Failed to send data to ThingSpeak, got status:%s, reason:%s" % (response.status,response.reason))
        conn.close()


class IOTRouter(object):

    def __init__(self,gateways=[]):
        self.iot_queue = defer.DeferredQueue()
        self.iot_gateways = {}
        for gw in gateways:
            self.register_gateway(gw)
        self.setup()

    def setup(self,_trash=None):
        self.iot_queue.get().addCallback(self.submit).addCallback(self.setup).addErrback(self.report_error)

    def register_gateway(self,gw):
        LOG.info("Registering gateway %s for message class %s" % (gw,gw.msg_class))
        self.iot_gateways.setdefault(gw.msg_class,[]).append(gw)

    def submit(self,msg):
        print msg
        print self.iot_gateways.keys()
        for gw in self.iot_gateways.get(msg.__class__,[]):
            LOG.info("Submitting message %s to gateway %s" % (msg,gw))
            gw.send(msg,msg.timestamp)

    def report_error(self,failure):
        LOG.error(failure)


