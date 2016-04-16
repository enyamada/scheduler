"""
   This module contains function that interface with AWS
"""

import boto3
import urllib2
from urllib2 import HTTPError, URLError
import base64


def create_spot_instance(config, job_id, sched_time, docker_image, env_vars):
    """
    Submits a request to AWS to create a spot instance at a scheduled date/time
    to run a container with optional env vars.

    Args:
        config: dict that must contain the ami-id, instance-type, key-name, sg-name and
            spot-price keys with the parameters to be passed to AWS.
        job_id: the scheduler job id of the corresponding job. This is important
            to create a user_data with the proper notification call to the server
            to let it know that the container started/ended.
        sched_time: the scheduled time the instance should be created. The expected
            format is YYYY-MM-DD HH:MM:SS (GMT tinmezone).
        docker_image: the image to be ran by docker.
        env_vars: string with the env vars to be supplied to docker. The format
            should be the expected by it (-e xx=yy -e zz=kk ...)
 
    Returns:
        A list with the spot instance request id, its state and status code.
    """

    client = boto3.client('ec2')

    # Get my own public fqdn by quering metadata
    my_own_name = urllib2.urlopen(
        "http://169.254.169.254/latest/meta-data/public-hostname").read()

    user_data = (
        "#!/bin/bash\n"
        "touch /tmp/start.txt\n"
        "curl -i -H 'Content-Type: application/json'  "
        "'http://%s/v1/notifications/%s?status=started' -X PUT\n"
        "yum -y update\n"
        "yum install docker -y\n"
        "sudo service docker start\n"
        "sudo docker run %s %s\n"
        "touch /tmp/executing.txt\n"
        "sleep 180\n"
        "curl -i -H 'Content-Type: application/json'  "
        "'http://%s/v1/notifications/%s?status=finished' -X PUT\n" %
        (my_own_name, job_id, env_vars, docker_image, my_own_name, job_id))

    response = client.request_spot_instances(
        SpotPrice="%s" % config["spot-price"],
        InstanceCount=1,
        Type='one-time',
        ValidFrom=sched_time,
        LaunchSpecification={
            'ImageId': config["ami-id"],
            'InstanceType': config["instance-type"],
            'KeyName': config["key-name"],
            'SecurityGroups': ['default', config["sg-name"]],
            'UserData': base64.b64encode(user_data)
        }
    )

    req_id = response['SpotInstanceRequests'][0]['SpotInstanceRequestId']
    req_state = response['SpotInstanceRequests'][0][
        'State']  # open/failed/active/cancelled/closed
    req_status_code = response['SpotInstanceRequests'][0][
        'Status']['Code']  # pending-evaluation/price-too-low/etc

    return [req_id, req_state, req_status_code]


def get_aws_req_status(req_id):
    """
    Get the latest status about the provided spot request indentified by the supplied id.
    The state, status code and instance id are returned as a list.
    """

    client = boto3.client('ec2')

    response = client.describe_spot_instance_requests(
        SpotInstanceRequestIds=[req_id]
    )

    req_state = response['SpotInstanceRequests'][0]['State']
    req_status_code = response['SpotInstanceRequests'][0]['Status']['Code']

    instance_id = response['SpotInstanceRequests'][0].get('InstanceId', None)

    return [req_state, req_status_code, instance_id]


def create_spot_security_group(sg_name):
    """
    Creates (if doesnt exist already) a security group with the provided name
    to be applied to the spot instance that ensures that
    all outgoing traffic to tcp/80 is allowed. This is important because to permit
    the instance to notify the server that the container started/ended
    """

    sg_desc = "Security group to be applied to any spot instance running our schedule jobs"

    client = boto3.client('ec2')

    # First verify if such a SG already exists. If so, just return its id
    try:
        response = client.describe_security_groups(GroupNames=[sg_name])
        return response["SecurityGroups"][0]["GroupId"]

    except:  # If there's no sg with such name

        # Create a new group and save its id
        response = client.create_security_group(
            GroupName=sg_name, Description=sg_desc)
        sg_id = response["GroupId"]

        # Add the rules
        response = client.authorize_security_group_egress(GroupId=sg_id, IpPermissions=[
            {'IpProtocol': 'tcp', 'FromPort': 80, 'ToPort': 80, 'IpRanges': [
                {'CidrIp': '0.0.0.0/0'}]}])

        # Return the SG id
        return sg_id


def terminate_instance(instance_id):
    """
    Terminates an instance (identified by the supplied instance_id)
    """

    client = boto3.client('ec2')
    response = client.terminate_instances(InstanceIds=instance_id)
