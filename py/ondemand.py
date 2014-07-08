#!/opt/kup/virtupy/bin/python
import datetime
import arrow
import sys
import common


def main(st,et):
    if not st:
        st = arrow.utcnow().replace(minutes=common.DEFAULT_LOOKBACK_MINUTES)
    if not et:
        et = arrow.utcnow()
    print "Processing from %s until %s" % (st, et)
    while True:
        if st>et:
            break
        ts = st.timestamp
        for inst_type in common.AWS_INSTANCE_METRICS:
            tags = {
                'inst_type' : inst_type,
                'units' : 'quantity',
                'cloud' : 'aws'
                }
            common.otsdb_send('aws_vcpu', common.AWS_INSTANCE_METRICS[inst_type]['vCPU'], tags, ts)
            common.otsdb_send('aws_ecu', common.AWS_INSTANCE_METRICS[inst_type]['ECU'], tags, ts)
            tags['units'] = 'GiB'
            common.otsdb_send('aws_memoryGiB', common.AWS_INSTANCE_METRICS[inst_type]['memoryGiB'], tags, ts)


        print "%s:\n\t%s regions" % (st, len(common.AWS_ON_DEMAND_PRICES.keys()))
        reg1 = common.AWS_ON_DEMAND_PRICES.keys()[0]
        print "\t%s products" % (len(common.AWS_ON_DEMAND_PRICES[reg1].keys()))
        prod1 = common.AWS_ON_DEMAND_PRICES[reg1].keys()[0]
        print "\t%s instance types" % (len(common.AWS_ON_DEMAND_PRICES[reg1][prod1].keys()))
        for region in common.AWS_ON_DEMAND_PRICES:
            print '\t%s zones in %s' % (len(common.AWS_REGIONS_TO_ZONES[region]), region)
            for zone in common.AWS_REGIONS_TO_ZONES[region]:
                for product in common.AWS_ON_DEMAND_PRICES[region]:
                    if not common.AWS_ON_DEMAND_PRICES[region][product]:
                        print "WARNING: Empty %s:%s" % (region, product)
                    for inst_type in common.AWS_ON_DEMAND_PRICES[region][product]:
                        price = common.AWS_ON_DEMAND_PRICES[region][product][inst_type]
                        tags = {
                            'region' : region,
                            'product' : product,
                            'inst_type' : inst_type,
                           'zone' : zone,
                            'units' : 'USD',
                            'cloud' : 'aws'
                            }
                        common.otsdb_send('price_ondemand', price, tags, ts)
                        tags['price_type'] = 'ondemand'
                        common.otsdb_send('price', price, tags, ts)      
             
                        if inst_type not in common.AWS_INSTANCE_METRICS:
                            print "ERROR: Cannot find metrics for %s" % inst_type

        st = st.replace(minutes=+1)

if __name__ == "__main__":
    st = et = None
    if len(sys.argv) > 1:
        st = arrow.Arrow.fromdatetime(datetime.datetime.strptime(sys.argv[1], "%Y%m%d"))
    if len(sys.argv) > 2:
        et = arrow.Arrow.fromdatetime(datetime.datetime.strptime(sys.argv[2], "%Y%m%d"))
    main(st,et)
