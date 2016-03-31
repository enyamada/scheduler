from flask import Flask, jsonify, abort, make_response, request, url_for
from time import strptime, strftime
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler

import boto3
import logging
import MySQLdb
import os
import time



# Configurable
# - sg_name
# - bid_price
# - polling interval



# REVIEW
#   hostname for callback

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


def save_job_schedule (docker_image, stime, callback, env_vars, req_id, req_state, req_status_code):

    # Collect the env vars if any was specified
    env_vars_text = ""
    if not env_vars is None:
       for k,v in env_vars.iteritems():
           env_vars_text = env_vars_text + k + "=" + v + "\n"

    cursor = Db.cursor()


    # Try to insert the new schedule into the database. If it fails, return -1, otherwise, return the scheduled job id.
    try:
       if callback == "":
          sql = 'INSERT INTO jobs(run_at, docker_image, status, env_vars, req_id, req_state, req_status_code) VALUES ("%s", "%s", "scheduled", "%s", "%s", "%s", "%s")' % (stime.strftime('%Y-%m-%d %H:%M:%S'), docker_image, env_vars_text, req_id, req_state, req_status_code)
       else:
          sql = 'INSERT INTO jobs(run_at, docker_image, status, env_vars, callback, req_id, req_state, req_status_code) VALUES ("%s", "%s", "scheduled", "%s", "%s", "%s", "%s", "%s")' % (stime.strftime('%Y-%m-%d %H:%M:%S'), docker_image, env_vars_text, callback, req_id, req_state, req_status_code)

       cursor.execute (sql)

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

    # Schedule the spot instance
    [ req_id, req_state, req_status_code ] = create_spot_instance (stime, json["docker_image"], env_vars)


    job_id = save_job_schedule (json["docker_image"], stime, callback, env_vars, req_id, req_state, req_status_code)

    if job_id == -1:
        return make_response(jsonify({'error': 'Something went wrong when attempting to save data into database'}), 500)


    # Schedule the job to run at the specified date/time
    scheduler.add_job(run_job, 'date', run_date=json["datetime"], args=[job_id])

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
       cursor = Db.cursor()
       cursor.execute ("UPDATE jobs SET status='%s' WHERE id=%s" % (status, job_id))
       if status == "finished":
           print ("*** Executing user callback function: %s ****" % callback(job_id))
           print "*** Terminating spot instance ***"

       return make_response(jsonify({'Success': 'Notification has been processed, status updated to %s' % status}), 200)

    except Exception as e:
       return make_response(jsonify({'Error': 'Something went wrong when updating DB - %s' % str (e)}), 500)





@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)

def open_db_connection():
   # TODO: save credentials in a safe place
   conn  =  MySQLdb.connect ("localhost", "scheduler", "Pass&&wOrd", "scheduler")
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



def create_spot_instance(sched_time):

    client  = boto3.client('ec2')

    response = client.request_spot_instances (
       SpotPrice     = '0.0102',
       InstanceCount = 1,
       Type          = 'one-time',
       ValidFrom     = sched_time,
       LaunchSpecification = {
         'ImageId'        : 'ami-1e159872',
         'InstanceType'   : 'm3.medium',
         'KeyName'        : 'enyamada-key-pair',
         'SecurityGroups' : ['default', sg_name],
         'UserData'       : """
#!/bin/bash
touch /tmp/start.txt
curl -i -H "Content-Type: application/json"  'http://ec2-54-233-149-166.sa-east-1.compute.amazonaws.com/v1/notifications/42?status=started' -X PUT
yum -y update
yum install docker -y
sudo service docker start
sudo docker run hello-world
touch /tmp/executing.txt
sleep 120
curl -i -H "Content-Type: application/json"  'http://ec2-54-233-149-166.sa-east-1.compute.amazonaws.com/v1/notifications/42?status=finished' -X PUT
"""
       }
    }


    request_id = response['SpotInstanceRequests'][0]['SpotInstanceRequestId']
    state =  response['SpotInstanceRequests'][0]['State']  # open/failed/active/cancelled/closed
    status_code = response['SpotInstanceRequests'][0]['Status']['Code'] # pending-evaluation/price-too-low/etc

    return [ request_id, state, status_code]




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
""" 
    # states: open|active|closed|cancelled|failed)
  
    # 1- for each job (not marked as done in DB) that should be already running  (run_at<NOW()) and db's state is open:
    # 	 - using the req_id, check if the req's state  
    #       - open: update the status code 
    # 	    - active: save the instance_id on db, status=>running
    #       - failed/cancelled: call callback with a special parameter?
    #       - closed: re-run immediately if it was terminated by AWS so that it runs ASAP


    When our callback is called:
	- ensure DB is consistent (state=active, instance_id is known)
        - call the user's callback
	- terminate the instance
	- DB status=> done  
      
    # 2- identify if any instance was already running has finished
    # Get the list of scheduled jobs that are not done yet and have schedule time > now


"""

    cursor = Db.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute ("SELECT * FROM jobs WHERE run_at <= NOW() AND status <> 'done' AND state='open'")

    rows = cursor.fetchall()
    for row in rows:
        job_id = row['id']

	[ state, status_code, instance_id ] = get_req_status (row['req_id'])
        if state == 'open':
	   update_db_state (job_id, state, status_code, None)
	elif state == 'active':
	   update_db_state (job_id, state, status_code, instance_id)
        elif state == 'cancelled' or state == 'failed':
	   notify_user (job_id, state)
	   update_db_state (job_id, state, status_code, instance_id)
	elif state == 'closed':
	   rerun (job_id)
        else:
	   print "Unexpected state: %s" % state


def update_db_status_code (job_id, status_code):

    cursor = Db.cursor()
    cursor.execute ("UPDATE jobs SET status_code='%s' where id=%s" % (status_code, job_id))



def update_db_state (job_id, state, status_code, instance_id):
   
    cursor = Db.cursor()
    if instance_id is None:
       cursor.execute ("UPDATE jobs SET state='%s', status_code='%s' where id=%s" % (state, status_code, job_id))
    else:
       cursor.execute ("UPDATE jobs SET state='%s', status_code='%s', instance_id='%s' WHERE id=%s" % (state, status_code, instance_id, job_id))
 

def notify_user (job_id, state):

    cursor = Db.cursor(MySQLdb.cursors.DictCursor)
   
    cursor.execute ("SELECT * from jobs where id=%s" % job_id)

    print ("Executing %s with parameter %state!" % row['callback'] 
 
	

def rerun (job_id):
	pass


def get_req_status (req_id)
    
    client  = boto3.client('ec2')

    response = client.describe_spot_instance_requests (
          SpotInstanceRequestIds=[req_id]
    )

    state = response['SpotInstanceRequests'][0]['State'] 
    status_code = response['SpotInstanceRequests'][0]['Status']['Code'] 
    if response['SpotInstanceRequests'][0].has_key ('InstanceId'): 
        instance_id = response['SpotInstanceRequests'][0]['InstanceId']
    else:
        instance_id = None

    return [ state, status_code, instance_id ] 


     


if __name__ == '__main__':
    logging.basicConfig()
    Db = open_db_connection()


    sg_name = "spot-temp-sg-xx"
    spot_sg_id = create_spot_security_group(sg_name)

    scheduler = BackgroundScheduler()
    scheduler.add_job(check_jobs, 'interval', seconds=60)

    scheduler.start()

    app.run(debug=True, host='0.0.0.0', port=80)

