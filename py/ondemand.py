import datetime
import arrow
import sys
import common

def main(st,et):
    if not st:
        st = arrow.utcnow().replace(minutes=-8)
    if not et:
        et = arrow.utcnow()
    print "Processing from %s until %s" % (st, et)
    while True:
        if st>et:
            for region in common.AWS_ON_DEMAND_PRICES:
                for product in common.AWS_ON_DEMAND_PRICES[region]:
                    for inst_type in common.AWS_ON_DEMAND_PRICES[region][product]:
                        price = common.AWS_ON_DEMAND_PRICES[region][product][inst_type]
                        for zone in common.AWS_REGIONS_TO_ZONES[region]:
                            tags = {
                            'region' : region,
                            'product' : product,
                            'inst_type' : inst_type,
                            'price_type' : 'ondemand',
                            'zone' : zone,
                            'units' : 'USD'
                            }
                            ts = st.timestamp
                            common.otsdb_send('price', price, tags, ts)
            for inst_type in common.AWS_INSTANCE_METRICS:
                tags = {
                    'inst_type' : inst_type,
                    'units' : 'quantity'
                    }
                common.otsdb_send('aws_vcpu', common.AWS_INSTANCE_METRICS['vCPU'], tags, ts)
                common.otsdb_send('aws_ecu', common.AWS_INSTANCE_METRICS['ecu'], tags, ts)
                tags['units'] = 'GiB'
                common.otsdb_send('aws_memoryGiB', common.AWS_INSTANCE_METRICS['memoryGiB'], tags, ts)
                    
        st = st.replace(minutes=+1)

if __name__ == "__main__":
    st = et = None
    if len(sys.argv) > 1:
        st = arrow.Arrow.fromdatetime(datetime.datetime.strptime(sys.argv[1], "%Y%m%d"))
    if len(sys.argv) > 2:
        et = arrow.Arrow.fromdatetime(datetime.datetime.strptime(sys.argv[2], "%Y%m%d"))
    main(st,et)
