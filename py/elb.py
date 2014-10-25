#!/opt/kup/virtupy/bin/python
import sys
import urllib2
import simplejson
import common
import arrow
import boto.ec2
import boto.ec2.cloudwatch
import boto.ec2.elb
from copy import deepcopy
from cm_api.api_client import ApiResource

cwcon = ec2con = None

def main():
    global ec2con
    global cwcon

    time_f = arrow.utcnow().replace(minutes=common.DEFAULT_LOOKBACK_MINUTES)
    time_t = arrow.utcnow()
    
    for region in ['us-east-1','us-west-1','eu-west-1']:
        ec2con = boto.ec2.connect_to_region(region)
        elbcon = boto.ec2.elb.connect_to_region(region)
        cwcon = boto.ec2.cloudwatch.CloudWatchConnection()
        elbs = elbcon.get_all_load_balancers()
        print "Found %s ELBs in %s" % (len(elbs), region)
        for elb in elbs:
            elb_name = elb.name
            print elb_name
            if 'unitcore' not in elb_name.lower():
                print 'Ignoring...'
                continue
            elb_stat = cwcon.get_metric_statistics(300,
                                                   time_f,
                                                   time_t,
                                                   'RequestCount',
                                                   'AWS/ELB',
                                                   'Sum',
                                                   { 'LoadBalancerName' : elb_name })     
            elb_tags = {'region' : region,
                        'elb'   : elb_name }
            for s in elb_stat:
                ts = common.ts_from_aws(s)
                val = s['Sum']
                common.otsdb_send('elb_cum_reqs', val, elb_tags, ts, False)
                print "Average request count: %s" % val
            elb_cum_cores = 0
            elb_cum_util = 0
            
            instances0 = elb.instances
            inst_cnt = len(instances0)
            print "Found %s instances in this ELB" % inst_cnt
            for inst0 in instances0:
                inst_id = inst0.id
                # print "Getting info on %s" % inst_id
                ress = ec2con.get_all_reservations(filters={'instance-id' : inst_id})
                print len(ress)
                if len(ress) > 0:
                    print "Found %s reservations for %s: %s" % (len(ress), inst_id, ress)
                    for res in ress:
                        instances = res.instances
                        for inst in instances:
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

                            tags = {}
                            tags['elb'] = elb_name
                            tags['product'] = product
                            tags['region'] = inst.region.name
                            tags['zone'] = inst.placement
                            inst_type = inst.instance_type.replace('.','_')

                            tags['inst_type'] = inst_type
                            vcpus = int(common.AWS_INSTANCE_METRICS[inst_type]['vCPU'])
                            print "%s has %s vcpus" % (inst_type, vcpus)
                            # print "Adding %s to %s" % (vcpus, elb_cum_cores)
                            elb_cum_cores += vcpus
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

                            # print 'Fetching stats for %s: %s' % (inst_id, stat)
                            if stat:
                                for s in stat:
                                    ts = common.ts_from_aws(s)
                                    avg_cpu = float(s['Average'])
                                    common.otsdb_send('avg_cpu_util',
                                              avg_cpu,
                                              tags,
                                              ts,
                                              False)
                                    common.otsdb_send('avg_cpu_util',
                                              avg_cpu,
                                              tags,
                                              ts,
                                              False)
                                    norm_util = avg_cpu/vcpus
                                    elb_cum_util += avg_cpu
                                    print "%s/%s=%s" % (avg_cpu, vcpus, norm_util)
                                    common.otsdb_send('avg_cpu_util_norm',
                                              avg_cpu*vcpus,
                                              tags,
                                              ts,
                                              False)

                            else:
                                print "No stats found for %s" % inst_id
                
        common.otsdb_send('elb_cum_cores', 
                          elb_cum_cores,
                          elb_tags,
                          ts,
                          False)
        elb_cum_util = elb_cum_util/float(inst_cnt)
        common.otsdb_send('elb_cum_util', 
                          elb_cum_util,
                          elb_tags,
                          ts,
                          False)
        elb_cum_util_norm = elb_cum_util / elb_cum_cores
        print "%s/%s=%s" % (elb_cum_util, elb_cum_cores, elb_cum_util_norm)



        common.otsdb_send('elb_cum_util_norm', 
                          elb_cum_util_norm,
                          elb_tags,
                          ts,
                          False)


if __name__ == "__main__":
    main()
