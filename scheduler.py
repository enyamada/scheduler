from flask import Flask, jsonify, abort, make_response, request, url_for
from time import strptime, strftime
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler

import base64
import logging
import logging.handlers
import os
import time
import yaml

import urllib2
from urllib2 import HTTPError, URLError
import boto3
import MySQLdb



# Configurable
# - sg_name
# - bid_price
# - polling interval



# REVIEW
#   hostname for callback
#   security: hashes, login

app = Flask(__name__)


# POST /v1/schedule
#       Data :
#       {
#               "docker_image": "xx",
#               "date_time": "xxx",
#               "env-variables" : {
#                       "env1": "e1",
#                       "env2": "e2"
#               }
#       }


# POST /v1/schedule?docker_image=xxx&datetime=yyy&ENV='ENV1=xx,ENV2=yy'
# GET  /v1/list : list all scheduled jobs
# GET  /v1/status&jobid=xxx
# POST /v1/callback


# POST /v1/jobs - schedule a new job
# GET  /v1/jobs - list all scheduled jobs
# GET  /v1/jobs/xx - list status of a specific job
# PUT  /v1/jobs/xx - change callback (or run_at)


# TODO
# Timezone



# flask
# abort (400) - exits with 400
# return jsoninfy (xx), 201 - returns a json + 201


@app.route('/')
def hello_world():
    return 'Hello World!'



def update_db_req_data (job_id, req_id, req_state, req_status_code):

    cursor = Db.cursor()
    cursor.execute ("UPDATE jobs SET req_id='%s', req_state='%s', req_status_code='%s' WHERE id=%s" % (req_id, req_state, req_status_code,  job_id))


def save_job_schedule (docker_image, stime, callback, env_vars):

    cursor = Db.cursor()

    print "** env_cars = %s " % env_vars
    # Try to insert the new schedule into the database. If it fails, return -1, otherwise, return the scheduled job id.
    try:
       if callback == "":
          sql = "INSERT INTO jobs(run_at, docker_image, status, env_vars) VALUES ('%s', '%s', 'scheduled', '%s' )" % (stime.strftime('%Y-%m-%d %H:%M:%S'), docker_image, MySQLdb.escape_string(env_vars))
       else:
          sql = 'INSERT INTO jobs(run_at, docker_image, status, env_vars, callback) VALUES ("%s", "%s", "scheduled", "%s", "%s")' % (stime.strftime('%Y-%m-%d %H:%M:%S'), docker_image, env_vars, callback)

       cursor.execute ( sql )

    except Exception as e:
        print str(e)
        return -1


    # get id
    cursor.execute ("SELECT last_insert_id()")
    id = cursor.fetchone()
    return id[0];




@app.route ('/v1/jobs', methods=['POST'])
def schedule_job():

    # Make sure a well formed json was posted
    try:
       json = request.get_json()
    except:
        return jsonify ({"Error": "Bad request. Make sure that the JSON posted is well formed."}), 400

    # Return an error if there is any mandatory field missing
    if not (json.has_key("docker_image") and json.has_key("datetime")):
        return jsonify ({"Error": "docker_image or datetime is missing"}), 400


    # TODO: Ensure datetime makes sense (correct format and it's in the future)

    # Verify if time format is ok and stores in into a time-tuple format
    try:
        stime = datetime.strptime (json["datetime"], "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return jsonify ({"Error": "Date format must be yyyy-mm-dd hh:mm:ss"}), 400

    # Ensure time is in the future
    if stime < datetime.now():
        return jsonify ({"Error": "Date/time must be in the future"}), 400

    # Get callback if it was sent
    if json.has_key("callback"):
        callback = json["callback"]
    else:
        callback = ""

    # Get the env_vars is it was sent
    env_vars = None
    if json.has_key("env_vars"):
       if type(json["env_vars"]) is dict:
          env_vars =  json["env_vars"]

    # Convert the env vars to a notation format to be accepeted by docker ("-e 'X1=v1' -e 'X2=v2' etc)
    env_vars = build_env_vars_docker_format (env_vars)

    job_id = save_job_schedule (json["docker_image"], stime, callback, env_vars )
    if job_id == -1:
        return make_response(jsonify({'error': 'Something went wrong when attempting to save data into database'}), 500)

    # Schedule the spot instance
    [ req_id, req_state, req_status_code ] = create_spot_instance (config["aws"], job_id, stime, json["docker_image"], env_vars)
    update_db (job_id, req_id=req_id, req_state=req_state, req_status_code=req_status_code)
    #update_db_req_data (job_id, req_id, req_state, req_status_code)

    # Schedule the job to run at the specified date/time
    #scheduler.add_job(run_job, 'date', run_date=json["datetime"], args=[job_id])

    # Returns a json with the accepted json data and the job-id appended
    json["job_id"] = job_id
    return jsonify (json), 201



@app.route ('/v1/jobs', methods=['GET'])
def get_list():

    # Get a dict containing all the jobs
    cursor = Db.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute ("SELECT * FROM jobs WHERE run_at > NOW()")

    # Respond with a json containing all the data
    return jsonify (scheduled_jobs = cursor.fetchall()), 200



@app.route ('/v1/jobs/<job_id>')
def get_status(job_id) :


    # Get data about the specific job
    cursor = Db.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute ("SELECT * FROM jobs WHERE id = %s" % job_id)

    # 404 if no such job was found
    if cursor.rowcount == 0:
       return make_response(jsonify({'error': 'No job with such id was found'}), 404)

    # Otherise, return the job data
    return jsonify (job  = cursor.fetchone())



@app.route ('/v1/jobs/<job_id>', methods=['PUT'])
def update_job (job_id):

    # TODO: for now, only callback may be updated

    # Make sure a well formed json was posted
    try:
       json = request.get_json()
    except:
        return jsonify ({"Error": "Bad request. Make sure that the JSON posted is well formed."}), 400

    # Job exists and it's still time to change? (it's not already running or it's done?)
    cursor = Db.cursor()
    cursor.execute ("SELECT * FROM jobs WHERE id = %s AND status='scheduled'" % job_id)
    if cursor.rowcount == 0:
       return make_response(jsonify({'error': 'No job with such id was found or the job is already running or done'}), 404)


    # Try to update
    try:
       cursor.execute ("UPDATE jobs SET callback='%s' WHERE id=%s" % (json["callback"], job_id))
       return make_response(jsonify({'Success': 'Callback function updated to %s' % json["callback"]}), 200)

    except Exception as e:
       return make_response(jsonify({'Error': 'Something went wrong when updating DB - %s' % str (e)}), 500)





@app.route ('/v1/notifications/<job_id>', methods=['PUT'])
def process_notification (job_id):

    status = request.args.get('status')
    if status == None:
        return make_response(jsonify({'Error': 'Missing status= parameter'}), 400)

    # Try to update
    try:
       update_db (job_id, status=status)

       if status == "finished":
           print ("*** Executing user callback function: %s ****" % callback(job_id))
           call_callback(job_id)
           print "*** Terminating spot instance ***"
           terminate_instance(job_id)
           update_db (job_id, status='done')

       return make_response(jsonify({'Success': 'Notification has been processed, status updated to %s' % status}), 200)

    except Exception as e:
       return make_response(jsonify({'Error': 'Something went wrong when updating DB - %s' % str (e)}), 500)





@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)

def open_db_connection(config):

   conn = MySQLdb.connect (config["host"], config["user"], config["secret"], config["db"])
   conn.autocommit(True)
   return conn


def run_job (job_id):
    print "%s: executing job %d" % (datetime.now(), job_id)
    cursor = Db.cursor()

    # Get the job's data from the DB
    sql = 'SELECT docker_image, env_vars FROM jobs WHERE id=%s' % job_id
    cursor.execute (sql)
    row = cursor.fetchone()
    print "Start instance running %s with vars %s" % (row[0], row[1])


    # Spin up the EC2 instance

    # Update job status
    cursor.execute ("UPDATE jobs SET status='requested' WHERE id=%s" % job_id);



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




def callback (job_id):
    cursor = Db.cursor()
    cursor.execute ("SELECT callback FROM jobs WHERE id = %s " % job_id)

    row = cursor.fetchone()
    return row[0]



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
       SpotPrice     = config["spot-price"], 
       InstanceCount = 1,
       Type          = 'one-time',
       ValidFrom     = sched_time,
       LaunchSpecification = {
         'ImageId'        : config["ami-id"], 
         'InstanceType'   : config["instance-type"], 
         'KeyName'        : config["key-name"],
         'SecurityGroups' : ['default', sg_name],
         'UserData'       : base64.b64encode (user_data)
       }
    )


    req_id          = response['SpotInstanceRequests'][0]['SpotInstanceRequestId']
    req_state       = response['SpotInstanceRequests'][0]['State']  # open/failed/active/cancelled/closed
    req_status_code = response['SpotInstanceRequests'][0]['Status']['Code'] # pending-evaluation/price-too-low/etc

    return [ req_id, req_state, req_status_code]




# Status
#    submitted | running | done
#
# States:
#    open (request state)
#      pending-evaluation (status code)
#      pending-fulfillment
#      capacity-not-available, capacity-oversubscribed
#      price-too-low
#      not-scheduled-yet
#      launch-group-constraing
#      az-group-constraint
#      placement-group-constraing
#      constraint-not-fullfilable
#    active
#      fulfilled
#    closed
#      scheduled-expired
#      bad-parameters
#      system-errors
#      instance-terminated-by-price
#    cancelled
#      cancelled-before-fulfillment
#    failed
#      bad-parameters

def check_jobs():
    # states: open|active|closed|cancelled|failed)
  
    # 1- for each job (not marked as done in DB) that should be already running  (run_at<NOW()) and db's state is open:
    # 	 - using the req_id, check if the req's state  
    #       - open: update the status code 
    # 	    - active: save the instance_id on db, status=>running
    #       - failed/cancelled: call callback with a special parameter?
    #       - closed: re-run immediately if it was terminated by AWS so that it runs ASAP


    # When our callback is called:
    #   - ensure DB is consistent (state=active, instance_id is known)
    #   - call the user's callback
    #   - terminate the instance
    #   - DB status=> done  
      
    # 2- identify if any instance was already running has finished
    # Get the list of scheduled jobs that are not done yet and have schedule time > now



    cursor = Db.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute ("SELECT * FROM jobs WHERE run_at <= NOW() AND status <> 'done' AND (req_state='active' OR req_state='open')")

    rows = cursor.fetchall()
    for row in rows:
        print "** PROCESSING row "
        print row
        job_id = row['id']
        [ aws_req_state, aws_req_status_code, aws_instance_id ] = get_aws_req_status (row['req_id'])


        if aws_req_state == 'open':
            update_db (job_id, req_state=aws_req_state, req_status_code=aws_req_status_code)
	    #update_db_state (job_id, aws_req_state, aws_req_status_code, None)
	elif aws_req_state == 'active':
            update_db (job_id, req_state=aws_req_state, req_status_code=aws_req_status_code, instance_id=aws_instance_id)
	    #update_db_state (job_id, aws_req_state, aws_req_status_code, aws_instance_id)
        elif aws_req_state == 'cancelled' or aws_req_state == 'failed':
	    notify_user (job_id, aws_req_state)
            update_db (job_id, req_state=aws_req_state, req_status_code=aws_req_status_code, instance_id=aws_instance_id)
	    #update_db_state (job_id, aws_req_state, aws_req_status_code, aws_instance_id)
	elif aws_req_state == 'closed':
	    rerun (job_id)
        else:
	    print "Unexpected state: %s" % aws_req_state


def update_db_status_code (job_id, status_code):

    cursor = Db.cursor()
    cursor.execute ("UPDATE jobs SET status_code='%s' where id=%s" % (status_code, job_id))



def update_db_state (job_id, aws_req_state, aws_req_status_code, aws_instance_id):
   
    cursor = Db.cursor()

    print "*** UPDATE id=%d, state=%s, status_code=%s, instance_id=%s" % (job_id, aws_req_state, aws_req_status_code, aws_instance_id)

    # Update the db with the latest aws request state and status_code
    if aws_instance_id is None:
        cursor.execute ("UPDATE jobs SET req_state='%s', req_status_code='%s' where id=%s" % (aws_req_state, aws_req_status_code, job_id))
    else:
        cursor.execute ("UPDATE jobs SET req_state='%s', req_status_code='%s', instance_id='%s' WHERE id=%s" % (aws_req_state, aws_req_status_code, aws_instance_id, job_id))

    # In addition, if the state is cancelled or failed, mark this job as done, so that we'll know there's nothing else to do here.
    if aws_req_state == 'cancelled' or aws_req_state == 'failed':
        cursor.execute ("UPDATE jobs SET status='done' WHERE id=%s" % (job_id))

 

def notify_user (job_id, req_state):

    cursor = Db.cursor(MySQLdb.cursors.DictCursor)
   
    cursor.execute ("SELECT * from jobs where id=%s" % job_id)
    row = cursor.fetchone()

    print ("Executing %s with parameter %s!" % (row['callback'],req_state) )
 
	

def rerun (job_id): 
    return 0


def get_aws_req_status (req_id):
    
    client  = boto3.client('ec2')

    response = client.describe_spot_instance_requests (
          SpotInstanceRequestIds=[req_id]
    )

    req_state       = response['SpotInstanceRequests'][0]['State'] 
    req_status_code = response['SpotInstanceRequests'][0]['Status']['Code'] 

    if response['SpotInstanceRequests'][0].has_key ('InstanceId'): 
        instance_id = response['SpotInstanceRequests'][0]['InstanceId']
    else:
        instance_id = None

    return [ req_state, req_status_code, instance_id ] 


     
def terminate_instance (job_id):

    cursor = Db.cursor(MySQLdb.cursors.DictCursor)

    cursor.execute ("SELECT * from jobs where id=%s" % job_id)
    row = cursor.fetchone()

    client = boto3.client('ec2')

    print "Teminating %s" % row['instance_id']
    response = client.terminate_instances (InstanceIds=[row['instance_id']])

    cursor.execute ("UPDATE jobs SET status='done' WHERE id=%s" % job_id)
 


def update_db (job_id, **kwargs):

    cursor = Db.cursor()

    set_clause = ""
    for k in kwargs:
        set_clause = set_clause + "%s='%s'," % (k, kwargs[k])

    set_clause = set_clause[:-1]

    sql = "UPDATE jobs SET %s WHERE id=%s" % (set_clause, job_id)
    print "Update db: %s" % sql
    cursor.execute (sql)
 
    

def call_callback (job_id):

  
    url = callback(job_id)
   
    try:
        f = urllib2.urlopen(url)
        f.close()
    
    except Exception as e:
        update_db (job_id, notes="Something went wrong when trying to callback %s: %s" % (url, e.message))

    
    except URLError as e:
        print "Wrong url"
        update_db (job_id, notes="Tried to callback %s but seems like an invalid url: %s" % (url, e.reason))

    else: 
	update_db (job_id, notes="Called back %s sucessfully at %s" % (url, datetime.now()))



def build_env_vars_docker_format (env_vars):

    # env_vars is a hash

    # Collect the env vars if any was specified
    env_vars_parameter = ""
    if not env_vars is None:
       for k,v in env_vars.iteritems():
           env_vars_parameter = env_vars_parameter + "-e '%s=%s' " % (k,v)


    return env_vars_parameter



def setup_logging (config):


    logger = logging.getLogger("")
    logger.setLevel(logging.DEBUG)
    handler = logging.handlers.RotatingFileHandler(
                 config["file"], maxBytes=config["max-bytes"], backupCount=config["backup-count"]
    )
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)


if __name__ == '__main__':

    CONFIG_FILE = 'scheduler.yaml'

    with open (CONFIG_FILE, "r") as f:
        config = yaml.load (f)

    print config

    setup_logging (config["log"])
    Db = open_db_connection(config["db"])

    
    spot_sg_id = create_spot_security_group(config["aws"]["sg-name"])

    scheduler = BackgroundScheduler()
    scheduler.add_job(check_jobs, 'interval', seconds=config["app"]["polling-interval"])
    scheduler.start()

    app.run(debug=True, host='0.0.0.0', port=80)

