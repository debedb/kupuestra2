#!/opt/kup/virtupy/bin/python
import common
import boto.ec2
import boto.ec2.connection


_FMT = 'YYYY-MM-DDTHH:mm:ss.000Z'


def main():
    machines = common.AWS_ON_DEMAND_PRICES['us_east_1']
    f = open('/opt/kup/www/instance_types.txt', 'w')
    for m in machines:
        f.write(common.normalize_key(m.replace('.','_')))
        f.write("\n")
    f.close()
    ec2con = boto.ec2.connection.EC2Connection()
    regions = boto.ec2.regions()
    rf = open('/opt/kup/www/regions.txt','w')
    zf = open('/opt/kup/www/zones.txt','w')
    zoneset = set()
    for r in regions:
        rname = r.name
        rcon = boto.ec2.connect_to_region(rname)
        try:
            zones = rcon.get_all_zones()
        except Exception, e:
            print "Cannot get zones for %s: %s" % (rname, e)
            continue
        rname =  common.normalize_key(rname)
        for z in zones:
            zname = common.normalize_key(z.name)
            zname=zname.replace(rname,'')
            zoneset.add(zname)
        rf.write("%s\n" % (rname))
    zones_list = list(zoneset)
    zones_list.sort()
    [zf.write("%s\n" % z) for z in zones_list]
    zf.close()
    rf.close()
    

if __name__ == "__main__":
    main()
