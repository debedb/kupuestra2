import arrow
import boto.ec2
import boto.ec2.cloudwatch

CPU_M = 'Metric:CPUUtilization'

def main():
    ec2con = boto.ec2.connect_to_region('us-east-1')
    instances = ec2con.get_all_reservations(filters={'private-dns-name' : 'ip-10-146-185-5.ec2.internal'})
    print instances
                                                  
    cwc = boto.ec2.cloudwatch.CloudWatchConnection()
    # nmetrics = cwc.list_metrics()
    #    print metrics
    cpu_stat = cwc.get_metric_statistics(3600,
                              arrow.utcnow().replace(minutes=-5),
                              arrow.utcnow(),
                              CPU_M,
                              'AWS/EC2',
                              ['Average','Minimum','Maximum'],
                              { 'InstanceId' :1 })          
                              

    end_time = arrow.utcnow()


if __name__ == "__main__":
    main()
