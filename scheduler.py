from flask import Flask, jsonify, abort, make_response, request, url_for
from flask import Flask, jsonify, abort, make_response, request, url_for
from time import strptime, strftime
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler

import base64
import logging
import logging.handlers
import os
import time


import urllib2
from urllib2 import HTTPError, URLError
import boto3
import MySQLdb

import aws
from config import read_config



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
    return 'Hello World!\n'




def save_job_schedule (docker_image, stime, callback, env_vars):

    cursor = Db.cursor()

    # Try to insert the new schedule into the database. If it fails, return -1, otherwise, return the scheduled job id.
    try:
       if callback == "":
          sql = "INSERT INTO jobs(run_at, docker_image, status, env_vars) VALUES ('%s', '%s', 'scheduled', '%s' )" % (stime.strftime('%Y-%m-%d %H:%M:%S'), docker_image, MySQLdb.escape_string(env_vars))
       else:
          sql = 'INSERT INTO jobs(run_at, docker_image, status, env_vars, callback) VALUES ("%s", "%s", "scheduled", "%s", "%s")' % (stime.strftime('%Y-%m-%d %H:%M:%S'), docker_image, env_vars, callback)

       logging.debug ("Saving job: %s" % sql)
       cursor.execute ( sql )

    except Exception as e:
        logging.critical ("Error when saving job into DB: %s" % str(e))
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
    callback = json.get("callback", "")

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
    [ req_id, req_state, req_status_code ] = aws.create_spot_instance (config["aws"], job_id, stime, json["docker_image"], env_vars)
    update_db (job_id, req_id=req_id, req_state=req_state, req_status_code=req_status_code)

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
           logging.info ("Executing job %s user callback function: %s" % (job_id, callback(job_id)) )
           call_callback(job_id)
           logging.info ( "Terminating job %s spot instance" % job_id) 

           cursor.execute ("SELECT * from jobs where id=%s" % job_id)
           row = cursor.fetchone()
           aws.terminate_instance(row['instance_id'])

           logging.info ( "Marking job %s as done" % job_id) 
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





def callback (job_id):
    cursor = Db.cursor()
    cursor.execute ("SELECT callback FROM jobs WHERE id = %s " % job_id)

    row = cursor.fetchone()
    return row[0]







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
        logging.info  ("Polling job %s status with AWS" % row['id']) 
        logging.debug ("DB row: %s" % row)

        job_id = row['id']
        [ aws_req_state, aws_req_status_code, aws_instance_id ] = aws.get_aws_req_status (row['req_id'])


        if aws_req_state == 'open':
            update_db (job_id, req_state=aws_req_state, req_status_code=aws_req_status_code)
	elif aws_req_state == 'active':
            update_db (job_id, req_state=aws_req_state, req_status_code=aws_req_status_code, instance_id=aws_instance_id)
        elif aws_req_state == 'cancelled' or aws_req_state == 'failed':
            update_db (job_id, req_state=aws_req_state, req_status_code=aws_req_status_code, instance_id=aws_instance_id)
	elif aws_req_state == 'closed':
	    rerun (job_id)
        else:
	    logging.info ("Unexpected state returned from AWS at check_jobs(): %s" % aws_req_state)





def rerun (job_id): 
    return 0



def update_db (job_id, **kwargs):

    cursor = Db.cursor()

    set_clause = ""
    for k in kwargs:
        set_clause = set_clause + "%s='%s'," % (k, kwargs[k])

    set_clause = set_clause[:-1]

    sql = "UPDATE jobs SET %s WHERE id=%s" % (set_clause, job_id)
    logging.debug ("Update db: %s" % sql)
    cursor.execute (sql)
 
    

def call_callback (job_id):

  
    url = callback(job_id)
   
    try:
        f = urllib2.urlopen(url)
        f.close()
    
    except Exception as e:
        update_db (job_id, notes="Something went wrong when trying to callback %s: %s" % (url, e.message))

    
    except URLError as e:
        logging.info ("Error when calling back job %s callback function (%s)" % (job_id, url) )
        update_db (job_id, notes="Tried to callback %s but seems like an invalid url: %s" % (url, e.reason))

    else: 
        logging.info ("Job %s callback function (%s) called successfully" % (job_id, url) )
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

    if config["level"] == "debug":
        level = logging.DEBUG
    elif config["level"] == "info":
        level = logging.INFO
    elif config["level"] == "warning":
        level = logging.WARNING
    elif config["level"] == "error":
        level = logging.ERROR
    elif config["level"] == "critical":
        level = logging.CRITICAL
    else:
        level = logging.WARNING

    logger.setLevel(level)
    handler = logging.handlers.RotatingFileHandler(
                 config["file"], maxBytes=config["max-bytes"], backupCount=config["backup-count"]
    )
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)





if __name__ == '__main__':


    config = read_config()
    logging.debug ("Config read: %s" % config)

    setup_logging (config["log"])
    Db = open_db_connection(config["db"])

    
    spot_sg_id = aws.create_spot_security_group(config["aws"]["sg-name"])

    scheduler = BackgroundScheduler()
    scheduler.add_job(check_jobs, 'interval', seconds=config["app"]["polling-interval"])
    scheduler.start()

    app.run(host='0.0.0.0', port=80)






from time import strptime, strftime
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from config import read_config

import base64
import logging
import logging.handlers
import os
import time

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
    return 'Hello World!\n'




def save_job_schedule (docker_image, stime, callback, env_vars):

    cursor = Db.cursor()

    # Try to insert the new schedule into the database. If it fails, return -1, otherwise, return the scheduled job id.
    try:
       if callback == "":
          sql = "INSERT INTO jobs(run_at, docker_image, status, env_vars) VALUES ('%s', '%s', 'scheduled', '%s' )" % (stime.strftime('%Y-%m-%d %H:%M:%S'), docker_image, MySQLdb.escape_string(env_vars))
       else:
          sql = 'INSERT INTO jobs(run_at, docker_image, status, env_vars, callback) VALUES ("%s", "%s", "scheduled", "%s", "%s")' % (stime.strftime('%Y-%m-%d %H:%M:%S'), docker_image, env_vars, callback)

       logging.debug ("Saving job: %s" % sql)
       cursor.execute ( sql )

    except Exception as e:
        logging.critical ("Error when saving job into DB: %s" % str(e))
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
    callback = json.get("callback", "")

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
    [ req_id, req_state, req_status_code ] = aws.create_spot_instance (config["aws"], job_id, stime, json["docker_image"], env_vars)
    update_db (job_id, req_id=req_id, req_state=req_state, req_status_code=req_status_code)

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
           logging.info ("Executing job %s user callback function: %s" % (job_id, callback(job_id)) )
           call_callback(job_id)
           logging.info ( "Terminating job %s spot instance" % job_id) 
           terminate_instance(job_id)
           logging.info ( "Marking job %s as done" % job_id) 
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






def callback (job_id):
    cursor = Db.cursor()
    cursor.execute ("SELECT callback FROM jobs WHERE id = %s " % job_id)

    row = cursor.fetchone()
    return row[0]






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
        logging.info  ("Polling job %s status with AWS" % row['id']) 
        logging.debug ("DB row: %s" % row)

        job_id = row['id']
        [ aws_req_state, aws_req_status_code, aws_instance_id ] = aws.get_aws_req_status (row['req_id'])


        if aws_req_state == 'open':
            update_db (job_id, req_state=aws_req_state, req_status_code=aws_req_status_code)
	elif aws_req_state == 'active':
            update_db (job_id, req_state=aws_req_state, req_status_code=aws_req_status_code, instance_id=aws_instance_id)
        elif aws_req_state == 'cancelled' or aws_req_state == 'failed':
            update_db (job_id, req_state=aws_req_state, req_status_code=aws_req_status_code, instance_id=aws_instance_id)
	elif aws_req_state == 'closed':
	    rerun (job_id)
        else:
	    logging.info ("Unexpected state returned from AWS at check_jobs(): %s" % aws_req_state)





def rerun (job_id): 
    return 0




     
def update_db (job_id, **kwargs):

    cursor = Db.cursor()

    set_clause = ""
    for k in kwargs:
        set_clause = set_clause + "%s='%s'," % (k, kwargs[k])

    set_clause = set_clause[:-1]

    sql = "UPDATE jobs SET %s WHERE id=%s" % (set_clause, job_id)
    logging.debug ("Update db: %s" % sql)
    cursor.execute (sql)
 
    

def call_callback (job_id):

  
    url = callback(job_id)
   
    try:
        f = urllib2.urlopen(url)
        f.close()
    
    except Exception as e:
        update_db (job_id, notes="Something went wrong when trying to callback %s: %s" % (url, e.message))

    
    except URLError as e:
        logging.info ("Error when calling back job %s callback function (%s)" % (job_id, url) )
        update_db (job_id, notes="Tried to callback %s but seems like an invalid url: %s" % (url, e.reason))

    else: 
        logging.info ("Job %s callback function (%s) called successfully" % (job_id, url) )
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

    if config["level"] == "debug":
        level = logging.DEBUG
    elif config["level"] == "info":
        level = logging.INFO
    elif config["level"] == "warning":
        level = logging.WARNING
    elif config["level"] == "error":
        level = logging.ERROR
    elif config["level"] == "critical":
        level = logging.CRITICAL
    else:
        level = logging.WARNING

    logger.setLevel(level)
    handler = logging.handlers.RotatingFileHandler(
                 config["file"], maxBytes=config["max-bytes"], backupCount=config["backup-count"]
    )
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)





if __name__ == '__main__':


    config = read_config()
    logging.debug ("Config read: %s" % config)

    setup_logging (config["log"])
    Db = open_db_connection(config["db"])

    
    spot_sg_id = aws.create_spot_security_group(config["aws"]["sg-name"])

    scheduler = BackgroundScheduler()
    scheduler.add_job(check_jobs, 'interval', seconds=config["app"]["polling-interval"])
    scheduler.start()

    app.run(host='0.0.0.0', port=80)






