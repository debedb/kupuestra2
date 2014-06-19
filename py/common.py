import atexit
import calendar
import dateutil.parser
import datetime
import socket
import arrow
import time
import collections

OPENTSDB_HOST = 'node2.cloudera1.enremmeta.com'
OPENTSDB_PORT = 4242

OPENTSDB_FILENAME = 'otsdb_%s.txt' % datetime.date.today().strftime('%m%d%Y_%M%h%s')
OPENTSDB_FILE = None
global OPENTSDB_FILE

from pricing import *

def if_send(key, value, ts=None):
    pass

OTSDB_BUFFER_SIZE = 1000

OTSDB_BUFFER = []

records_to_otsdb = 0

def send_messages(host, port, msg):
    i = 1
    global records_to_otsdb
    while True:
        try:
            sock = socket.socket()
            sock.connect((OPENTSDB_HOST, OPENTSDB_PORT))
            if isinstance(msg, collections.Iterable):
                # print "Sending %s messages to %s:%s" % (len(msg), host, port)
                for m in msg:
                    # print "To socket: %s" % m
                    sock.sendall(m)
                    # print m
                records_to_otsdb += len(msg)
            else:
                # print "To socket: %s" % msg
                sock.sendall(msg)
                # print msg
                records_to_otsdb += 1
            sock.close()
            break
        except Exception, e:
            if i > 5:
                raise
            print  e
            time.sleep(i*i)
            print "Trying again %s" % i
            i += 1
            continue
    

def otsdb_send(key, value, tags, ts=None, file_only=False):
    if ts is None:
        ts = arrow.utcnow().timestamp
    tags_str = ' '.join(["%s=%s"  % (normalize_key(k),normalize_key(v)) for k,v in tags.items()])
    msg = "put %s %s %s %s" % (key, ts, value, tags_str)
    # print "Sending to OpenTSDB:\n\t%s" % msg
    msg += "\n"
    # print msg
    OTSDB_BUFFER.append(msg)
    if len(OTSDB_BUFFER) % OTSDB_BUFFER_SIZE == 0:
        send_messages(OPENTSDB_HOST, OPENTSDB_PORT, OTSDB_BUFFER)
        OTSDB_BUFFER[:] = []

def otsdb_send_remaining():
    if OTSDB_BUFFER:
        print "Sending remaining %s messages"  % len(OTSDB_BUFFER)
        print "Ending with\n\t%s" % OTSDB_BUFFER[-1]
        send_messages(OPENTSDB_HOST, OPENTSDB_PORT, OTSDB_BUFFER)
    print "Wrote to OpenTSDB: %s" % records_to_otsdb
    print "Done."

atexit.register(otsdb_send_remaining)            

def g_send(key, value, ts=None):
    if ts is None:
        ts = arrow.utcnow().timestamp
    msg = "%s %s %s" % (key,value,ts)
    #print ts
#    print msg 
    msg += "\n"
    sock = socket.socket()
    sock.connect((CARBON_SERVER, CARBON_PORT))
    sock.sendall(msg)
    sock.close()

def normalize_key(key):
    key = key.replace('/','_')
    key = key.replace('-','_')
    key = key.replace(' ','_')
    key = key.replace(':','_')
    key = key.replace('(','')
    key = key.replace(')','')
    return key

def ts_from_aws(arg):
    arg0 = arg
    if isinstance(arg, dict):
        arg = arg['Timestamp']
    if not isinstance(arg, datetime.datetime):
      #  print "Parsing %s"  %arg
        arg = dateutil.parser.parse(arg)
    ts = calendar.timegm(arg.utctimetuple())
    #print "Parsed %s to %s" % (arg0, ts)
    return ts

DEFAULT_LOOKBACK_MINUTES=-8
