#!/usr/bin/env python

"""
This script launches a AWS instance that will run the scheduler server.
"""

import sys
import os
from time import sleep
import getopt
import boto3
import ConfigParser
import botocore


def create_http_security_group(sg_name, options):
    """
    Creates (if doesnt already exist) a SG with the specified name and options
    and returns its id.
    """

    sg_desc = "Security group to be applied to any spot instance running our schedule jobs"

    client = boto3.client('ec2',
                          aws_access_key_id=options['aws_access_key_id'],
                          aws_secret_access_key=options['aws_secret_access_key'])

    # First verify if such a SG already exists. If so, just return its id
    try:
        response = client.describe_security_groups(GroupNames=[sg_name])
        return response["SecurityGroups"][0]["GroupId"]

    except botocore.exceptions.NoCredentialsError:
        print "AWS credentials failed"
        sys.exit(3)

    except botocore.exceptions.ClientError as e:  # If there's no sg with such name

        # Credentials wrong?
        if e.response['Error']['Code'] == 'AuthFailure':
            print "AWS credentials failed"
            sys.exit(3)

        # Create a new group and save its id
        response = client.create_security_group(
            GroupName=sg_name, Description=sg_desc)
        sg_id = response["GroupId"]

        # Add the rules
        response = client.authorize_security_group_ingress(GroupId=sg_id, IpPermissions=[
            {'IpProtocol': 'tcp', 'FromPort': 80, 'ToPort': 80,
             'IpRanges': [{'CidrIp': '0.0.0.0/0'}]}])

        # Return the SG id
        return sg_id


def create_instance(sg_name, options):
    """
    Creates an AWS EC2 instance using the specified options
    """

    client = boto3.client("ec2")

    txt = open("my-init.sh")
    user_data = txt.read()
    user_data = user_data % (options["aws_access_key_id"], options[
                             "aws_secret_access_key"])

    key_name = options["key_name"]

    try:
        response = client.run_instances(
            ImageId="ami-c229c0a2",
            MinCount=1,
            MaxCount=1,
            InstanceType="t2.micro",
            SecurityGroups=["default", sg_name],
            KeyName=key_name,
            UserData=user_data
        )

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'InvalidKeyPair.NotFound':
            print "Key pair name(%s) was not accepted. " % key_name
            sys.exit(4)

    instance_id = response["Instances"][0]["InstanceId"]

    # Wait for the public dns name gets ready. This is normally quick
    public_dns_name = ""
    while public_dns_name == "":
        print "Hold on..."
        sleep(10)
        response = client.describe_instances(InstanceIds=[instance_id])
        public_dns_name = response["Reservations"][
            0]["Instances"][0]["PublicDnsName"]

    return [instance_id, public_dns_name]


def print_help():

    """ 
    Print help 
    """

    print 'Options: '
    print '  -k|--key-name <AWS key name to be used for provisioning>'
    print '  --aws-access-key-id <aws key id>'
    print '  --aws-secret-access-key <aws access key>'
    print ''
    print 'All options are mandatory, except if the corresponding env vars were provided'
    print '(AWS_KEY_NAME, AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY_NAME)'


def get_options(argv):
    """
    Get the options provided as command line args. If any of them were not specified,
    then try to find them out using other means.
    """

    key_name = ""
    aws_access_key_id = ""
    aws_secret_access_key = ""

    try:
        opts, args = getopt.getopt(argv, "hk:", ["key-name=", "aws-access-key-id=",
                                                 "aws-secret-access-key="])

        for opt, arg in opts:
            if opt == '-h':
                print_help()
                sys.exit()
            elif opt == '-k' or opt == '--key-name':
                key_name = arg
            elif opt == '--aws-access-key-id':
                aws_access_key_id = arg
            elif opt == '--aws-secret-access-key':
                aws_secret_access_key = arg

    except getopt.GetoptError:
        print_help()
        sys.exit(2)

    # If key name was not provided via command line, try to see if there's an
    # env var
    if key_name == "":
        key_name = os.environ.get("AWS_KEY_NAME")
        if key_name is None:
            print '-k <key-name> is a mandatory parameter (or use the AWS_KEY_NAME var)'
            sys.exit(2)

    if aws_access_key_id == "" or aws_secret_access_key == "":
        [aws_access_key_id, aws_secret_access_key] = get_aws_credentials()

    conf = dict(aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                key_name=key_name)

    return conf


def get_aws_credentials():
    """
    Gets the AWS credentials to be used. First look if env vars were defined,
    then search the ~/.aws/credentials file. Bail out if nothing was found.
    """

    # First: Are there env vars?
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if aws_access_key_id is not None and aws_secret_access_key is not None:
        return [aws_access_key_id, aws_secret_access_key]

    # Otherwise, try to read ~/.aws/credentials
    config = ConfigParser.ConfigParser()
    config.read(os.path.expanduser("~/.aws/credentials"))

    if config.has_option("default", "aws_access_key") and \
       config.has_option("default", "aws_secret_access_key"):
        aws_access_key_id = config.get("default", "aws_access_key")
        aws_secret_access_key = config.get("default", "aws_secret_access_key")
        return [aws_access_key_id, aws_secret_access_key]

    # Otherwise, this is an error, user needs to fix it.
    else:
        print "No AWS_ env variables or ~/.aws/credential file with default section was found."
        print "Please provide credentials either via --aws-access-key-id and "\
              "--aws-secret-access-key"
        print "options or through one of the ways above mentioned."
        sys.exit(2)


def main(argv):

    options = get_options(argv)

    # Create a new security group to ensure that HTTP incoming traffic will be
    # allowed
    sg_name = "http-in-sg-tmp"
    create_http_security_group(sg_name, options)

    # Create a new instance
    [instance_id, host_name] = create_instance(sg_name, options)

    print "Enjoy: Your server is %s. Please allow 10 min approx before testing." % (host_name)
    print
    print "Examples:"
    print
    print "To schedule a new job to run 'hello-world' container at YYYY-MM-DD HH:MM:SS (GMT)"
    print "(passing the env1 and env2 vars) and that should callback http://xxx.com when finished:"
    print
    print "curl -i -H \"Content-Type: application/json\" " \
          "http://%s/v1/jobs -X POST -d " \
          "'{\"docker_image\": \"hello-world\", \"datetime\": \"YYYY-MM-DD HH:MM:SS\", "  \
          "\"callback\":\"http://xxx.com\", \"env_vars\":{\"env1\": \"v1\", \"env2\": \"v2\"}}'" \
          % host_name
    print
    print
    print "To check a job status:"
    print
    print "curl -i http://%s/v1/jobs/<job-id>" % host_name
    print
    print
    print "To update a job callback to http://yyy.com:"
    print
    print "curl -i -H \"Content-Type: application/json\" " \
          "http://%s/v1/jobs/<job-id> -X PUT -d " \
          "'{\"callback\":\"http://yyy.com\"}'" % host_name


if __name__ == "__main__":
    main(sys.argv[1:])
