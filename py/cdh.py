#!/opt/kup/virtupy/bin/python
import sys
import urllib2
import simplejson
import common
import arrow
import boto.ec2
import boto.ec2.cloudwatch
from copy import deepcopy
from cm_api.api_client import ApiResource

cwcon = ec2con = None

CM_HOST = 'node0.cloudera1.enremmeta.com'

def get_nodes():
    nodes = fetch("cluster/nodes")
    return nodes

def main():
    global ec2con
    global cwcon

    ec2con = boto.ec2.connect_to_region('us-east-1')
    cwcon = boto.ec2.cloudwatch.CloudWatchConnection()

    api = ApiResource(CM_HOST, username="admin", password="admin")

    displayName = None
    for c in api.get_all_clusters():
        displayName = c.displayName
        print "Cluster: %s (%s)" % (displayName, c.name)
    
    inst_cache = {}

    insts = api.get_all_hosts('full')
    print "Found %s in the cluster" % [inst.hostId for inst in insts.objects]
    for inst in insts.objects:
        clusterName =  inst.roleRefs[0].clusterName
        if clusterName <> c.name:
            print 'Clusters do not correspond: %s vs %s' % (clusterName, c.name)
            continue

        cores = inst.numCores
        inst_id = inst.hostId
        inst_cache[inst_id] = my_cache =  {}
        # For later - we'll send in one data point for every TS query
        # that has AWS data
        my_cache['aws_info_recorded'] = False
        # my_cache['healthSummary'] = inst.healthSummary

        ress = ec2con.get_all_reservations(filters={'instance-id' : inst_id})
        if len(ress) > 0:
            print "Found %s reservations for %s: %s" % (len(ress), inst_id, ress)
        res = ress[0]

        instances = res.instances
        if len(instances) > 1:
            print "Found %s instances for %s %s" % (len(instances), inst_id, instances)
        inst = instances[0]
        if inst.id <> inst_id:
            raise Exception("%s != %s" % (inst.id, inst_id))

        platform = inst.platform
        vpc_id = inst.vpc_id

        if platform == 'windows':
            product = 'Windows'
        elif not platform:
            product = 'Linux_UNIX'
        else:
            product = 'UNKNOWN'
        if vpc_id:
            product += "_Amazon_VPC"

        ami = inst.image_id

        my_cache['product'] = product
        my_cache['region'] = inst.region.name
        my_cache['zone'] = inst.placement
        inst_type = inst.instance_type.replace('.','_')

        my_cache['inst_type'] = inst_type
        
        time_f =  arrow.utcnow().replace(minutes=common.DEFAULT_LOOKBACK_MINUTES)
        time_t = arrow.utcnow()
        # TODO
        # http://arr.gr/blog/2013/08/monitoring-ec2-instance-memory-usage-with-cloudwatch/
        # http://blog.sciencelogic.com/netflix-steals-time-in-the-cloud-and-from-users/03/2011
        # https://www.stackdriver.com/cpu-steal-why-aws-cloudwatch-metrics-are-different-than-agent-metrics/
        stat = cwcon.get_metric_statistics(300,
                                           time_f,
                                           time_t,
                                           'CPUUtilization',
                                           'AWS/EC2',
                                           ['Average','Minimum','Maximum'],
                                           { 'InstanceId' : inst_id })     
            # [{u'Timestamp': datetime.datetime(2014, 4, 13, 6, 5), u'Average': 0.35250000000000004, u'Minimum': 0.33, u'Maximum': 0.42, u'Unit': u'Percent'}]
        print 'Fetching stats for %s: %s' % (inst_id, stat)
        if stat:
            for s in stat:
                ts = common.ts_from_aws(s)
                my_cache['avg_cpu'] = float(s['Average'])
        else:
            print "No stats found for %s" % inst_id
    print "Querying CDH."
    series = api.query_timeseries('SELECT * WHERE clusterName = %s'  % c.name)
    for entry in series.objects[0].timeSeries:
        # print entry.metadata.__dict__
        metric = entry.metadata.metricName
        # internal host
        hostname = ""
        if 'hostname' in entry.metadata.attributes:
            host = entry.metadata.attributes['hostname']
            
        inst_id = ""
        my_cache = {}

        if 'hostId' in entry.metadata.attributes:
            inst_id = entry.metadata.attributes['hostId']
            if inst_id not in my_cache:
                print "Cannot find %s in %s" % (inst_id, inst_cache)
            my_cache = inst_cache[inst_id]
        service_name = ""
        if 'serviceName' in entry.metadata.attributes:
            service_name = entry.metadata.attributes['serviceName']
        service_type = ""
        if 'serviceType' in entry.metadata.attributes:
            service_type= entry.metadata.attributes['serviceType']
        role_type = ""
        if 'roleType' in entry.metadata.attributes:
            role_type = entry.metadata.attributes['roleType']

        
        num = entry.metadata.unitNumerators
        denom = entry.metadata.unitDenominators
        if len(num) > 1:
            print "Num:" + num
        if len(denom)>1:
            print "Denom:" + denom
        unit = num[0]
           
        if len(denom) > 0:
            unit += denom[0]
        tags = {
            'cdh_service_name_service_type_role_type' : "%s.%s.%s" % (
                service_name,
                service_type,
                role_type),
            'unit' : unit
            }
        
        combined_tags = deepcopy(tags)
        if my_cache:
            # combined_tags['healthSummary']= my_cache['healthSummary']
            combined_tags['inst_type'] = my_cache['inst_type']
            combined_tags['cloud'] = 'aws'
            combined_tags['region'] = my_cache['region']
            combined_tags['zone'] = my_cache['zone']
            combined_tags['product'] = my_cache['product']
            
        if not entry.data:
            continue
        
        for sample in entry.data:
            ts = arrow.Arrow.fromdatetime(sample.timestamp).timestamp
            val = sample.value
            if len(combined_tags) > 8:
                print "ERROR: Too many tags: %s" % combined_tags
                sys.exit(0)
            common.otsdb_send(metric, val, combined_tags, ts, False)
            # Do the AWS once only
            if my_cache and not my_cache['aws_info_recorded']:
                # print my_cache
                combined_tags['unit'] = 'percent'
                if 'avg_cpu' in my_cache:
                    common.otsdb_send('aws_average_cpu_utilization', 
                                      my_cache['avg_cpu'],
                                      combined_tags, 
                                      my_cache['ts'], 
                                      False)

if __name__ == "__main__":
    main()
