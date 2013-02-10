import sys
import logging
FMT = '%(asctime)s - %(module)-10s %(levelname)-8s %(message)s'
logging.basicConfig(level=logging.DEBUG, format=FMT)
#logging.basicConfig(level=logging.DEBUG, format='%(module)-10s %(levelname)-8s %(message)s')
logger = logging.getLogger('iot')
hdlr = logging.FileHandler('log/iot.log')
formatter = logging.Formatter(FMT)
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.DEBUG)

#import logging
