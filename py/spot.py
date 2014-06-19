#!/opt/kup/virtupy/bin/python
import calendar
import time
import datetime
import sys
import botocore.session
import arrow
import dateutil.parser
from aws_pricing import *
from pricing import *
import common
from pandas import Series, to_datetime

_FMT = 'YYYY-MM-DDTHH:mm:ss.000Z'

def main(st, et):
    if st:
        start_time = st
    else:
        start_time = arrow.utcnow().replace(minutes=common.DEFAULT_LOOKBACK_MINUTES)

    if et:
        end_time = et
    else:
        end_time = arrow.utcnow()

    all_regions = set()
    all_product_descriptions = set()
    all_instance_types = set()
    all_instance_zones = set()

    session = botocore.session.get_session()
    ec2 = session.get_service('ec2')
    operation = ec2.get_operation('DescribeSpotPriceHistory')
    local_timeseries = {}

    vals = {}
    tss = {}
    print 'Preparing...'
    for region in AWS_ON_DEMAND_PRICES:
        reg_key = region.replace('-','_')
        if region not in vals:
            vals[reg_key] = {}
            tss[reg_key] = {}
        for zone in AWS_REGIONS_TO_ZONES[region]:
            # print 'Zone: %s' % zone
            if zone not in vals[reg_key]:
                vals[reg_key][zone] = {}
                tss[reg_key][zone] = {}
            for product in AWS_ON_DEMAND_PRICES[region]:
                # print 'Product: %s' % product
                if not AWS_ON_DEMAND_PRICES[region][product]:
                    print "WARNING: Empty %s:%s" % (region, product)
                    continue
                if product not in vals[reg_key][zone]:
                    vals[reg_key][zone][product] = {}
                    tss[reg_key][zone][product] = {}
                for inst_type in common.AWS_ON_DEMAND_PRICES[region][product]:
                    # print "%s/%s/%s/%s" % (reg_key, zone, product, inst_type)
                    vals[reg_key][zone][product][inst_type] = []
                    tss[reg_key][zone][product][inst_type] = []
    #sys.exit(1)
    for region in ec2.region_names:
        all_regions.add(region)
        cnt = 0
        next_token = None
        print 'Collecting spot prices from region: %s for %s to %s' % (region, start_time.format(_FMT), end_time.format(_FMT))
        sys.stdout.flush()
        # if region != 'us-east-1':
        #continue
        while True:
            endpoint = ec2.get_endpoint(region)
            if next_token:
                response, data = operation.call(
                    endpoint,
                    start_time=start_time.format(_FMT),
                    end_time=end_time.format(_FMT),
                    next_token=next_token,
                )
            else:
                response, data = operation.call(
                    endpoint,
                    start_time=start_time.format(_FMT),
                )
            next_token = data.get('NextToken')
            spot_data = data.get('SpotPriceHistory', [])
            first_entry_in_batch = True
            sys.stdout.flush()
            for d in spot_data:
                
                ts = common.ts_from_aws(d)
                
                if first_entry_in_batch:
                    print "Fetched %s records starting with %s" % (len(spot_data), d['Timestamp'])
                    first_entry_in_batch = False
                
                # {u'Timestamp': '2014-04-10T23:49:21.000Z', u'ProductDescription': 'Linux/UNIX (Amazon VPC)', u'InstanceType': 'hi1.4xlarge', u'SpotPrice': '0.128300', u'AvailabilityZone': 'us-east-1b'}
                reg_key = region.replace('-','_')
                d['Region'] = reg_key
                
                
                d['InstanceTypeNorm'] = d['InstanceType'].replace('.','_')

                value = d['SpotPrice']

                zone = d['AvailabilityZone'].replace('-','_')
                product = d['ProductDescription'].replace('-','_').replace('(','').replace(')','_').replace(' ','_').replace('/','_')
                if product.endswith('_'):
                    product=product[:-1]
                inst_type = d['InstanceTypeNorm'].replace('-','_')

                tags = { 
                    'cloud' : 'aws',
                    'region' : reg_key,
                    'zone'  : zone,
                    'product' : product,
                    'inst_type' : inst_type,
                    'price_type' : 'spot',
                    'units' : 'USD'
                    }
                try:
                    vals[reg_key][zone][product][inst_type].append(value)
                    tss[reg_key][zone][product][inst_type].append(ts)
                except KeyError:
                    print "No on-demand info for %s/%s/%s/%s" % (reg_key,zone,product,inst_type)
                
                common.otsdb_send('price', value, tags, ts, False)
                cnt += 1

            if not next_token:
                break
        print "Found %s price points" % cnt
        for zone in tss[reg_key]:
            for product in tss[reg_key][zone]:
                for inst_type in tss[reg_key][zone][product]:
                    if not tss[reg_key][zone][product][inst_type]:
                        print "No spot info for %s/%s/%s/%s." % (reg_key, zone, product, inst_type)
                        continue
                    print "%s/%s/%s/%s" % (reg_key, zone, product, inst_type)
                    tags = { 
                        'cloud' : 'aws',
                        'region' : reg_key,
                        'zone'  : zone,
                        'product' : product,
                        'inst_type' : inst_type,
                        'price_type' : 'spot',
                        'units' : 'USD'
                        }

                    tss_ts = tss[reg_key][zone][product][inst_type]
                    tss_ts.sort()
                    tss_dt = to_datetime(tss_ts, unit='s')
                    s_data = vals[reg_key][zone][product][inst_type]
                    s1 = Series(s_data, tss_dt)
                    # print "Creating Series(%s, %s) from %s; length: %s" % (s_data, tss_dt, tss_ts, len(s1))

                    if len(s1) > 1:
                        # We already took care of 1-length (no fill)
                        s2 = s1.asfreq('1Min', method='ffill')
                        # print "Sparse series:\n%s\n" % s1
                        # print "Filled series:\n%s\n" % s2
                        for (dt,value) in s2.iteritems():
                            ts = arrow.Arrow.fromdatetime(dt).timestamp
                            common.otsdb_send('price', value, tags, ts, False)

        sys.stdout.flush()

if __name__ == "__main__":
    st = et = None
    if len(sys.argv) > 1:
        st = arrow.Arrow.fromdatetime(datetime.datetime.strptime(sys.argv[1], "%Y%m%d"))
    if len(sys.argv) > 2:
        et = arrow.Arrow.fromdatetime(datetime.datetime.strptime(sys.argv[2], "%Y%m%d"))
    main(st,et)
