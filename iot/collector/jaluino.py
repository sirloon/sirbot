from twisted.internet import defer, reactor

from txXBee.protocol import txXBee
import logging
LOG = logging.getLogger(__name__)


class JaluinoXBee(txXBee):
    """Jaluino board with Xbee configured as Coordinator. I receive and concentrate
       messages from Xbee routers. I have to know other Xbee devices with their address.
       I dispatch valid message to a deferred queue, from which some other component
       may process data more specifically.
       I also read from another deferred queue message I have to send, either to one 
       specific bee, or multiple.
    """

    def __init__(self, known_routers,*args, **kwds):
        super(JaluinoXBee, self).__init__(*args, **kwds)
        # this queue is used to collect message to send to xbee routers
        self.send_queue = defer.DeferredQueue()
        self.known_routers = {} # index by addr

        ##
        for router in known_routers:
            self.register_router(router)

        self.setup()

    def setup(self,_trash=None):
        self.send_queue.get().addCallback(self.send_data).addCallback(self.setup).addErrback(self.report_error)

    def register_router(self,router):
        # propagate sending queue to handelrs
        router.coord_queue = self.send_queue
        self.known_routers[router.addr] = router
        LOG.info("Registering router %s with address %s" % (router,router.addr))

    def handle_packet(self, xbee_packet):
        LOG.debug("Xbee packet: %s" % xbee_packet)
        source_addr = xbee_packet.get("source_addr_long", "default") 
        if source_addr in self.known_routers:
            router_handler = self.known_routers[source_addr]
            if router_handler:
                router_handler(xbee_packet)

    def send_data(self,msg):
        reactor.callFromThread(self.send,"tx",frame_id="\x01",
                     dest_addr_long=msg.get('addr'),dest_addr="\xff\xfe",
                     data=msg.get('data'))

    def report_error(self,failure):
        LOG.error(failure)



import re
import datetime


class XBeeRouterHandler(object):
    """
    Implements communications from and to XBee routers.
    """

    def __init__(self,addr):
        """addr: address of XBee router to handle"""
        raise Exception("Not implemented")

    def __call__(self,packet):
        """called when address matches current handler, packet is a raw
           dict xbee packet
        """
        raise Exception("Not implemented")


class JaluinoStationMessage(object):
    def __init__(self,flash=None,temp=None,hum=None,light=None,reset=None):
        self.datadict = {'flash':None, 'temp':None, 'hum':None, 'light':None, 'reset':0, 'watt':None}
        self.timestamp = None
    def items(self): # act as dict
        return self.datadict.items()
    def __repr__(self):
        return "<%s: @%s %s" % (self.__class__.__name__,self.datadict.items(),self.timestamp)

class JaluinoStation(XBeeRouterHandler):
    """
    Message looks like:

        !Field:YY-MM-DD hh:mm:ss|measure<cr><lf>

    """

    # groups: 1-Field, 2-Timestamp, 3-Measure
    MAGIC_RE = re.compile("^\!(\w+)\s*:\s*(\d+-\d+-\d+ \d+:\d+:\d+)\s*\|(.*)")

    def __init__(self,addr,coord_queue=None,iot_queue=None):
        self.addr = addr
        # will be set while attached to a coord
        self.coord_queue = coord_queue
        self.iot_queue = iot_queue
        self._current_msg = JaluinoStationMessage()

    def _parse_datetime(self,timestamp):
        date,time = timestamp.split()
        yy,mm,dd = map(int,date.split("-"))
        yyyy = 2000 + yy
        HH,MM,SS = map(int,time.split(":"))
        dt = datetime.datetime(yyyy,mm,dd,HH,MM,SS)
        return dt

    def parse_reply(self,line):
        m = self.MAGIC_RE.match(line)
        if m:
            field,timestamp,measure = m.groups()
            # normalize and convert common values
            measure = measure.strip() # clean  value from remaining '\r' from PIC
            timestamp = self._parse_datetime(timestamp)
            do_func = "do_%s" % field.lower()
            if hasattr(self,do_func):
                getattr(self,do_func)(field,timestamp,measure)
                self.submit() # try to submit current message
            else:
                LOG.warning("Received field '%s' but I got no func handler for it..." % field)
                return
        else:
            LOG.warning("Unkown reply/message: %s" % repr(line))

    def parse_question(self,line):
        # dirty
        now = datetime.datetime.now()
        strdt = now.strftime("%y-%m-%d %H:%M:%S")
        LOG.info("Sending datetime %s" % strdt)
        self._current_msg.datadict['reset'] += 1
        self.coord_queue.put({'addr' : self.addr, 'data' : strdt})

    def __call__(self,packet):
        if packet.get('rf_data'):
            line = packet['rf_data'].lstrip() # keep potential spaces on the right, could be part of message
            LOG.debug("Received line: %s" % repr(line))

            if not line.strip():
                LOG.error("Received empty line")
                return

            # reply/message
            if line.startswith("!"):
                self.parse_reply(line)
            # question (requiring a reply)
            elif line.startswith("?"):
                self.parse_question(line)
            # data sent to us, nothing to consider
            else:
                LOG.info("Informational message (I guess): %s" % repr(line))

    def do_flash(self,field,timestamp,measure):
        self._current_msg.datadict['flash'] = measure
        self._current_msg.datadict['watt'] = int(measure) * 60
        # always overwrite timestamp with last (anyway it's the same :))
        self._current_msg.timestamp = timestamp

    def do_roughtemperature(self,field,timestamp,measure):
        self._current_msg.datadict['temp'] = measure
        self._current_msg.timestamp = timestamp

    def do_light(self,field,timestamp,measure):
        self._current_msg.datadict['light'] = measure
        self._current_msg.timestamp = timestamp

    def do_reset(self,field,timestamp,measure):
        self._current_msg.datadict['reset'] = measure
        self._current_msg.timestamp = timestamp

    def do_humidity(self,field,timestamp,measure):
        self._current_msg.datadict['hum'] = measure
        self._current_msg.timestamp = timestamp

    def submit(self):
        if self._current_msg.datadict['flash'] and self._current_msg.datadict['temp'] and \
            self._current_msg.datadict['light'] and self._current_msg.datadict['hum']:
            try:
                if self.iot_queue:
                    LOG.info("Submitting %s to IOT gateways" % self._current_msg)
                    self.iot_queue.put(self._current_msg)
                else:
                    LOG.info("Ready to submit message %s but no IOT queue connected" % self._current_msg)
            finally:
                self._current_msg = JaluinoStationMessage()
        else:
            LOG.debug("Incomplete message, waiting: %s" % self._current_msg)
