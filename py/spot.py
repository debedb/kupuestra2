#!/opt/kup/virtupy/bin/python
import calendar
import time
import datetime
import sys
import botocore.session
import arrow
import dateutil.parser
import common

_FMT = 'YYYY-MM-DDTHH:mm:ss.000Z'

def main(st, et):
    if st:
        start_time = st
    else:
        start_time = arrow.utcnow().replace(minutes=-DEFAULT_LOOKBACK_MINUTES)

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

    for region in ec2.region_names:
        all_regions.add(region)
        cnt = 0
        next_token = None
        print 'Collecting spot prices from region: %s for %s to %s' % (region, start_time.format(_FMT), end_time.format(_FMT))
        sys.stdout.flush()
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
                key = "aws.%(Region)s.%(AvailabilityZone)s.%(ProductDescription)s.%(InstanceTypeNorm)s.price.spot" % d

                key = common.normalize_key(key)

                value = d['SpotPrice']
                tags = { 
                    'cloud' : 'aws',
                    'region' : d['Region'],
                    'zone'  : d['AvailabilityZone'],
                    'product' : d['ProductDescription'],
                    'inst_type' : d['InstanceTypeNorm'],
                    'price_type' : 'spot',
                    'units' : 'USD'
                    }
                common.otsdb_send('price', value, tags, ts, False)
                cnt += 1

            if not next_token:
                break
        print "Found %s price points" % cnt
        sys.stdout.flush()

if __name__ == "__main__":
    st = et = None
    if len(sys.argv) > 1:
        st = arrow.Arrow.fromdatetime(datetime.datetime.strptime(sys.argv[1], "%Y%m%d"))
    if len(sys.argv) > 2:
        et = arrow.Arrow.fromdatetime(datetime.datetime.strptime(sys.argv[2], "%Y%m%d"))
    main(st,et)
