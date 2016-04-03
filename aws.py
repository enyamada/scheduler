import boto3


def create_spot_instance(config, job_id, sched_time, docker_image, env_vars):

    client  = boto3.client('ec2')

    # Get my own public fqdn by quering metadata
    my_own_name = urllib2.urlopen("http://169.254.169.254/latest/meta-data/public-hostname").read()


    user_data = (
       b"#!/bin/bash\n"
       "touch /tmp/start.txt\n"
       "curl -i -H 'Content-Type: application/json'  'http://%s/v1/notifications/%s?status=started' -X PUT\n"
       "yum -y update\n"
       "yum install docker -y\n"
       "sudo service docker start\n"
       "sudo docker run %s %s\n"
       "touch /tmp/executing.txt\n"
       "sleep 180\n"
       "curl -i -H 'Content-Type: application/json'  'http://%s/v1/notifications/%s?status=finished' -X PUT\n"
       % (my_own_name, job_id, env_vars, docker_image, my_own_name, job_id)
    )


    response = client.request_spot_instances (
       SpotPrice     = "%s" % config["spot-price"],
       InstanceCount = 1,
       Type          = 'one-time',
       ValidFrom     = sched_time,
       LaunchSpecification = {
         'ImageId'        : config["ami-id"],
         'InstanceType'   : config["instance-type"],
         'KeyName'        : config["key-name"],
         'SecurityGroups' : ['default', config["sg-name"]],
         'UserData'       : base64.b64encode (user_data)
       }
    )


    req_id          = response['SpotInstanceRequests'][0]['SpotInstanceRequestId']
    req_state       = response['SpotInstanceRequests'][0]['State']  # open/failed/active/cancelled/closed
    req_status_code = response['SpotInstanceRequests'][0]['Status']['Code'] # pending-evaluation/price-too-low/etc

    return [ req_id, req_state, req_status_code]







def get_aws_req_status (req_id):

    client  = boto3.client('ec2')

    response = client.describe_spot_instance_requests (
          SpotInstanceRequestIds=[req_id]
    )

    req_state       = response['SpotInstanceRequests'][0]['State']
    req_status_code = response['SpotInstanceRequests'][0]['Status']['Code']

    instance_id = response['SpotInstanceRequests'][0].get ('InstanceId', None)

    return [ req_state, req_status_code, instance_id ]




def create_spot_security_group (sg_name):

    sg_desc = "Security group to be applied to any spot instance running our schedule jobs"

    client  = boto3.client('ec2')

    # First verify if such a SG already exists. If so, just return its id
    try:
       response = client.describe_security_groups (GroupNames=[sg_name])
       return  response["SecurityGroups"][0]["GroupId"]

    except: # If there's no sg with such name

       # Create a new group and save its id
       response = client.create_security_group (GroupName=sg_name, Description=sg_desc)
       sg_id = response ["GroupId"]

       # Add the rules
       response = client.authorize_security_group_egress(GroupId=sg_id, IpPermissions=[{'IpProtocol':'tcp', 'FromPort':80, 'ToPort':80,'IpRanges':[{'CidrIp':'0.0.0.0/0'}]}])

       # Return the SG id
       return sg_id





def terminate_instance (job_id):

    cursor = Db.cursor(MySQLdb.cursors.DictCursor)

    cursor.execute ("SELECT * from jobs where id=%s" % job_id)
    row = cursor.fetchone()

    client = boto3.client('ec2')

    logging.info ("Teminating job %s instance %s" % (job_id, row['instance_id']) )
    response = client.terminate_instances (InstanceIds=[row['instance_id']])







