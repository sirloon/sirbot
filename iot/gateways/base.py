import sys
import time as modtime
import datetime

from twisted.internet import defer, reactor
from twisted.internet.threads import deferToThread

import logging
LOG = logging


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



class IOTRouter(object):

    def __init__(self,gateways=[]):
        self.max_retry = 3
        self.interval = 10
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

    def submit(self,msg,retry=0):
        for gw in self.iot_gateways.get(msg.__class__,[]):
            LOG.info("Submitting message %s to gateway %s" % (msg,gw))
            deferToThread(gw.send,msg,msg.timestamp).addErrback(self.not_submitted,msg,retry)

    def not_submitted(self,failure,msg,retry):
        retry += 1
        if retry > self.max_retry:
            LOG.error("Failted to submit message %s for %s times, I'm giving up. Message lost !!! Error was:\n%s" % (msg,retry,failure))
        else:
            LOG.info("Failed submitting message %s on try #%s, try again in %s secs. Error was:\n%s" % (msg,retry,self.interval,failure))
            reactor.callLater(self.interval,self.submit,msg,retry)

    def report_error(self,failure):
        LOG.error(failure)


