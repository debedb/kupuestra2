# http://www.scalr.com/blog/how-to-get-ec2-pricing-data-programmatically       # 
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
    # "Linux_UNIX" :  "http://aws-assets-pricing-prod.s3.amazonaws.com/pricing/ec2/linux-od.js",
    "Linux_UNIX" : [ 
        "http://aws-assets-pricing-prod.s3.amazonaws.com/pricing/ec2/linux-od.js",
        "https://a0.awsstatic.com/pricing/1/ec2/linux-od.min.js"
        ],

    # "SUSE_Linux" : "http://aws-assets-pricing-prod.s3.amazonaws.com/pricing/ec2/sles-od.js",
    "SUSE_Linux" : [
        "http://aws-assets-pricing-prod.s3.amazonaws.com/pricing/ec2/sles-od.js",
        "http://a0.awsstatic.com/pricing/1/ec2/sles-od.min.js"
        ],
    
    # "Windows" : "http://aws-assets-pricing-prod.s3.amazonaws.com/pricing/ec2/mswin-od.js"
    "Windows" : 
    [ 
        "http://aws-assets-pricing-prod.s3.amazonaws.com/pricing/ec2/mswin-od.js",
        "http://a0.awsstatic.com/pricing/1/ec2/mswin-od.min.js"
        ]
}

AWS_REGIONS_TO_ZONES = {}

def callback(d):
    return d

def updateZones(rname, rname_x):
    if rname_x not in AWS_REGIONS_TO_ZONES:
        AWS_REGIONS_TO_ZONES[rname_x] = []
        print 'Connecting to %s' % rname_x
        ec2con = boto.ec2.connect_to_region(rname)
        AWS_REGIONS_TO_ZONES[rname_x] = [z.name.replace('-','_') for z in ec2con.get_all_zones()]
    else:
        # print "Already know zones for %s" % rname_x
        pass


def getRegionName(rinfo):
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
    return (rname, rname0)


def evalx(s):
    vers = 'vers'
    config = 'config'
    rate = 'rate'
    valueColumns = 'valueColumns'
    name = 'name'
    prices = 'prices'
    sizes = 'sizes'
    currencies = 'currencies'
    region = 'region'
    size = 'size'
    vCPU = 'vCPU'
    ECU = 'ECU'
    memoryGiB = 'memoryGiB'
    storageGB = 'storageGB'
    USD = 'USD'
    instanceTypes = 'instanceTypes'
    regions = 'regions'
    return eval(s)

def removeJsComments(s):
    # print s
    # sys.exit(0)
    s2 = ""

    in_comment = ""
    skip_next = False

    for i in range(len(s)-1):
        if skip_next:
            skip_next = False
            continue
        if in_comment:
            if s[i] == '*' and s[i+1] == '/':
                in_comment += s[i] + s[i+1]
                print "Skipped comment:\n%s\n" % in_comment
                skip_next = True
                in_comment = ""
                continue
            else:
                in_comment += s[i]
                continue
        if s[i] == '/' and s[i+1] == "*":
            in_comment = s[i] + s[i+1]
            skip_next = True
            continue
        else:
            s2 += s[i]
    if not skip_next:
        last_char = s[len(s)-1]
        if last_char == ';':
            pass
        else:
            print "Adding %s" % last_char
            s2 += last_char
    return s2

def updateMetrics(inst_type_x, sinfo):
    metrics = {
        "vCPU" : sinfo['vCPU'],
        "ECU"  : sinfo['ECU'],
        "memoryGiB": sinfo["memoryGiB"],
        # TODO parse storage
        "storageGB" : sinfo["storageGB"]
        }
    print "Writing metrics for %s: %s" % (inst_type_x, metrics)
    AWS_INSTANCE_METRICS[inst_type_x] = metrics

def parseUrl(product, suffix, url):
    f = urllib2.urlopen(url)
    s = f.read()
    s2 = removeJsComments(s)
    d = evalx(s2)
    regions = d['config']['regions']

    for rinfo in regions:
        (rname, rname0) = getRegionName(rinfo)
        rname_x = rname.replace('-','_')
        updateZones(rname, rname_x)
        for it in rinfo['instanceTypes']:
            for sinfo in it['sizes']:
                inst_type = sinfo['size']
                inst_type_x = inst_type.replace('.','_')
                            
                # Metrics - do this only once
                # print "::: %s/%s/%s" % (rname, product, suffix)
                if rname == 'us-east-1' and product == "Linux_UNIX" and not suffix:
                    updateMetrics(inst_type_x, sinfo)
                if rname_x not in AWS_ON_DEMAND_PRICES:
                    AWS_ON_DEMAND_PRICES[rname_x] = {}
                pname = "%s%s" % (product, suffix)
                if pname not in AWS_ON_DEMAND_PRICES[rname_x]:
                    AWS_ON_DEMAND_PRICES[rname_x][pname] = {}
                price = sinfo['valueColumns'][0]['prices']['USD']
                if inst_type_x in AWS_ON_DEMAND_PRICES[rname_x][pname]:
                    old_price = AWS_ON_DEMAND_PRICES[rname_x][pname][inst_type_x]
                    if price <> old_price:
                        # print "!!! WARNING: For %s/%s/%s: replacing price %s with %s" % (rname_x, pname, inst_type_x, old_price, price)
                        pass
                AWS_ON_DEMAND_PRICES[rname_x][pname][inst_type_x] = price

def fetch_aws_pricing():
    if AWS_INSTANCE_METRICS:
        print "Pricing known."
        return
    print "Fetching AWS pricing..."
    for product in AWS_PRODUCT_TO_URL:
        for suffix in ['', '_Amazon_VPC']:
            urls = AWS_PRODUCT_TO_URL[product]
            for url in urls:
                print "Parsing %s" % url
                parseUrl(product, suffix, url)


fetch_aws_pricing()
# sys.exit()
AWS_STANDARD_PRICES  = {
     'ondemand' : AWS_ON_DEMAND_PRICES
} 

