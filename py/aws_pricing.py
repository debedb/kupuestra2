# http://www.scalr.com/blog/how-to-get-ec2-pricing-data-programmatically
#
# http://stackoverflow.com/questions/7334035/get-ec2-pricing-programmatically

import urllib2
import simplejson as json
import sys
import boto.ec2

print "Loading AWS pricing..."

# product=Linux_UNIX
# product=Linux_UNIX_Amazon_VPC
# product=SUSE_Linux
# product=SUSE_Linux_Amazon_VPC
# product=Windows
# product=Windows_Amazon_VPC

# Instance type to metrics
AWS_INSTANCE_METRICS =  {}

# Region - product - instance type
AWS_ON_DEMAND_PRICES =  {}

AWS_PRODUCT_TO_URL = {
    "Linux_UNIX" : "http://aws-assets-pricing-prod.s3.amazonaws.com/pricing/ec2/linux-od.js",
    "SUSE_Linux" : "http://aws-assets-pricing-prod.s3.amazonaws.com/pricing/ec2/sles-od.js",
    "Windows" : "http://aws-assets-pricing-prod.s3.amazonaws.com/pricing/ec2/mswin-od.js"
}

AWS_REGIONS_TO_ZONES = {}

def callback(d):
    return d

def fetch_aws_pricing():
    if AWS_INSTANCE_METRICS:
        return
    for product in AWS_PRODUCT_TO_URL:
        for suffix in ['', '_Amazon_VPC']:
            url = AWS_PRODUCT_TO_URL[product]
            f = urllib2.urlopen(url)
            s = f.read()
            # print "Evaluating " + s
            d = eval(s)
            regions = d['config']['regions']

            for rinfo in regions:
                rname = rname0 = rinfo['region']
                rname_end = rname0[rname0.rindex('-')+1:]
                if rname0 in ['us-east', 'us-west']:
                    rname = "%s-1" % rname0
                elif rname0 == 'eu-ireland':
                    rname = 'eu-west-1'
                elif rname0 == 'apac-sin':
                    rname = 'ap-southeast-1'
                elif rname0 == 'apac-tokyo':
                    rname = 'ap-northeast-1'
                elif rname0 == 'apac-syd':
                    rname = 'ap-southeast-2'
                if rname0 <> rname:
                    print "Use %s instead of %s" % (rname, rname0)
                rname_x = rname.replace('-','_')
                if rname_x not in AWS_REGIONS_TO_ZONES:
                    AWS_REGIONS_TO_ZONES[rname_x] = []
                    print 'Connecting to %s' % rname
                    ec2con = boto.ec2.connect_to_region(rname)
                    AWS_REGIONS_TO_ZONES[rname_x] = [z.name.replace('-','_') for z in ec2con.get_all_zones()]
                for it in rinfo['instanceTypes']:
                    for sinfo in it['sizes']:
                        inst_type = sinfo['size']
                        inst_type_x = inst_type.replace('.','_')
                        # Metrics - do this only once
                        if rname == 'us-east' and product == "Linux_UNIX" and not suffix:
                            metrics = {
                                "vCPU" : sinfo['vCPU'],
                                "ECU"  : sinfo['ECU'],
                                "memoryGiB": sinfo["memoryGiB"],
                                # TODO parse storage
                                "storageGB" : sinfo["storageGB"]
                                }
                            AWS_INSTANCE_METRICS[inst_type_x] = metrics
                            # AWS_INSTANCE_METRICS[inst_type] = metrics

                        if rname_x not in AWS_ON_DEMAND_PRICES:
                            AWS_ON_DEMAND_PRICES[rname_x] = {}
                        pname = "%s%s" % (product, suffix)
                        if pname not in AWS_ON_DEMAND_PRICES[rname_x]:
                            AWS_ON_DEMAND_PRICES[rname_x][pname] = {}
                        AWS_ON_DEMAND_PRICES[rname_x][pname][inst_type_x] = sinfo['valueColumns'][0]['prices']['USD']
                        # AWS_ON_DEMAND_PRICES[rname_x][pname][inst_type] = sinfo['valueColumns'][0]['prices']['USD']

fetch_aws_pricing()

AWS_STANDARD_PRICES  = {
     'ondemand' : AWS_ON_DEMAND_PRICES
} 

