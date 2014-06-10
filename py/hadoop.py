#!/opt/kup/virtupy/bin/python
import sys
import urllib2
import simplejson
import common
import arrow
from jtscraper.jtscraper import JTScraper
import boto.ec2
import boto.ec2.cloudwatch

cwcon = ec2con = None

YARN_URL = "http://ec2-54-243-63-8.compute-1.amazonaws.com:8088/ws/v1/"
#JT_URL = "http://ec2-54-243-63-8.compute-1.amazonaws.com:50030/jobtracker.jsp"
JT_URL = "http://ec2-23-23-76-67.compute-1.amazonaws.com:50030/"


def fetch(svc):
    url = "%s%s" % (YARN_URL, svc)
    print "Connecting to %s" % url
    req = urllib2.Request(url)
    f = urllib2.urlopen(req)
    d = simplejson.load(f)
    f.close()
    return d

def get_nodes():
    
    nodes = fetch("cluster/nodes")
    return nodes

    

def main():
    global ec2con
    global cwcon


    ec2con = boto.ec2.connect_to_region('us-east-1')
    cwcon = boto.ec2.cloudwatch.CloudWatchConnection()

    jts = JTScraper(JT_URL, 'us-east-1')
    s = jts.clusterStats()
    print s

    jobs = s['jobs']
    for j in jobs:
        print 'Job %s' % j['job_name']
        job_info = jts.job(j['job_id'])
        print job_info
    
    print '*'* 50
    nodes = jts.nodes()
    print nodes

    for node in nodes:
        host = node['private_host']
        ress = ec2con.get_all_reservations(filters={'private-dns-name' : host})
        hadoop_id = host

        if ress:
            res = ress[0]
            instances = res.instances
            inst = instances[0]
            inst_id = inst.id
            inst_type = inst.instance_type
            key = "adotube.hadoop.hadoop1.nodes." + common.normalize_key(hadoop_id).replace('.','_')

            print "%s is %s (%s)" % (host, inst_id, inst_type)
            
            common.g_send(key + ".aws_vcpu.avail", common.AWS_INSTANCE_METRICS[inst_type]['vcpu'])
            # ecu = common.AWS_INSTANCE_METRICS[inst_type]['ecu']
            # common.g_send(key + ".aws_ecu.avail", ecu)
            common.g_send(key + ".aws_mem.avail", common.AWS_INSTANCE_METRICS[inst_type]['mem'])
            
            time_f =  arrow.utcnow().replace(minutes=-5)
            time_t = arrow.utcnow()
            print "Getting CloudWatch for data from %s to %s" % (time_f,time_t)
            # TODO
            # http://arr.gr/blog/2013/08/monitoring-ec2-instance-memory-usage-with-cloudwatch/
            # http://blog.sciencelogic.com/netflix-steals-time-in-the-cloud-and-from-users/03/2011
            # https://www.stackdriver.com/cpu-steal-why-aws-cloudwatch-metrics-are-different-than-agent-metrics/
            stat = cwcon.get_metric_statistics(300,
                              time_f,
                              time_t,
                              common.CW_M_CPU,
                              'AWS/EC2',
                              ['Average','Minimum','Maximum'],
                              { 'InstanceId' : inst_id })     
            # [{u'Timestamp': datetime.datetime(2014, 4, 13, 6, 5), u'Average': 0.35250000000000004, u'Minimum': 0.33, u'Maximum': 0.42, u'Unit': u'Percent'}]
            if stat:
                stat = stat[0]
                ts = common.ts_from_aws(stat)
                avg_cpu = float(stat['Average'])
                common.g_send(key + ".aws_cpu.util_avg_pct", avg_cpu, ts)
                common.g_send(key + ".aws_ecu.util_avg", avg_cpu*ecu, ts)
        
        #mem = node['availMemoryMB']
        #used_mem = node['usedMemoryMB']
        #hadoop_ts = node['lastHealthUpdate']
        #common.g_send(key + ".hadoop.mem_avail", mem, hadoop_ts)
        #common.g_send(key + ".hadoop.mem_used", used_mem, hadoop_ts)

# {u'nodes': {u'node': [{u'availMemoryMB': 6144, u'nodeHostName': u'ip-10-146-185-5.ec2.internal', u'lastHealthUpdate': 1397336871391, u'usedMemoryMB': 0, u'numContainers': 0, u'nodeHTTPAddress': u'ip-10-146-185-5.ec2.internal:8042', u'id': u'ip-10-146-185-5.ec2.internal:36344', u'healthReport': u'', u'state': u'RUNNING', u'healthStatus': u'Healthy', u'rack': u'/default-rack'}]}}

if __name__ == "__main__":
    main()
