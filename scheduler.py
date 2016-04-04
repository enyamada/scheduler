"""

   This is the main scheduler application module. It uses Flask, spinning up
   a web server and implements the REST API as proposed.

   The following operations are supported:

   POST /v1/jobs: Creates a new job. The specification must be sent using
       a JSON like this:

         {
               "docker_image": "xx",
               "date_time": "YYYY-MM-DD HH:MM:SS",
               "callback": "http://mycallback.com?xx",
               "env-variables": {
                       "env1": "e1",
                       "env2": "e2"
               }
         }

      ATTENTION: A "Content-Type: application/json" header
      must be sent by the client along with the request.

      If the request is accepeted, a JSON confirming the data and
      with a specific job id is returned:

         {
	     "callback": "http://usp.br",
	      "datetime": "2016-04-04 12:28:00",
              "docker_image": "hello-world",
   	      "env_vars": {
		    "env1": "v1",
		    "env2": "v2",
		    "env3": "v3"
		  },
	      "job_id": 5
          }

      This will schedule an AWS spot instance creation at the specified time
      that will run a container using the specified image and list of
      environment variables. Once the container finishes, the callback
      is executed.

      Note that the time will be interpreted using the UTC timezone.
      Both callback and env-variables fields are optional, everything else
      is mandatory.

    PUT  /v1/jobs/<job-id> - Change some data about the job specified
      (via job-id). Currently, only callback can be updated. A JSON
       must be sent with the proper data.

    GET /v1/jobs: Lists all scheduled jobs over the last 24h.

    GET /v1/jobs/<job-id>: List status of the specific job id(returned
        when the job was created)

        This is an example ouput:

      {
	"job": {
	   "callback": "http://usp2.br",
	   "docker_image": "hello-world",
           "env_vars": "-e 'env3=v3' -e 'env2=v2' -e 'env1=v1' ",
           "id": 6,
           "instance_id": "i-6e070eb4",
           "notes": "Something went wrong when trying to callback xxx",
           "req_id": "sir-03dhg71p",
           "req_state": "active",
           "req_status_code": "fulfilled",
           "run_at": "Mon, 04 Apr 2016 13:30:00 GMT",
           "status": "done"
      }

        req_id, req_state, req_status_code and instance_id are values
        provided by AWS and their meaning should be self explanatory.
        Those fields are kept as up to date as possible (this
        application polls AWS every 60 seconds (configurable) to
        check if any update has occurred).

        The possible status values are:
             - scheduled: job has been scheduled with AWS but has not
                   been effetively started yet.
             - started: job has started (spot instance has been created
                   and already started the specified container)
             - done: job has been done, no more processing is required.
             - re-scheduled: the job had been started but its instance was
                   terminated for whatever reason. For those cases,
                   the job is automatically re-scheduled. This status
                   reflects this scenario, and means the job was
                   scheduled again for a new run (but has not been started
                   yet)
                   means the


"""


from flask import Flask, jsonify, make_response, request
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler


import logging
import logging.handlers


import MySQLdb
import urllib2
from urllib2 import URLError

import aws
from config import read_config
import db


# Configurable
# - sg_name
# - bid_price
# - polling interval


# REVIEW
#   hostname for callback
#   security: hashes, login

app = Flask(__name__)


# TODO
# Timezone



@app.route('/')
def hello_world():
    """
    Handles the / request. Just to check the sevice is alive.
    """
    return 'Hello World!\n'


def save_job_schedule(db_conn, docker_image, stime, callback, env_vars):
    """
    Saves into the database the data about a new job.

    Args:
        db_conn: dabase connection (as returned by db.open_conneection()
        docker_image: container image to be used by the job
        stime: scheduled time (YYYY-MM-DD HH:MM:SS). It's assumed UTC tz.
        callback: callback endpoint to be called once the job is done.
        env_vars: environment variables to be passed to the container.
            They must be in the docker format ("-e 'VAR1'='val1'")

    Returns:
        job id.
    """

    cursor = db_conn.cursor()

    # Try to insert the new schedule into the database. If it fails, return
    # -1, otherwise, return the scheduled job id.
    try:
        if callback == "":
            sql = "INSERT INTO jobs(run_at, docker_image, status, " \
                  "env_vars) VALUES('%s', '%s', '%s', '%s' )" %(
                      stime.strftime('%Y-%m-%d %H:%M:%S'), docker_image,
                      STATUS_SCHEDULED, MySQLdb.escape_string(env_vars))

        else:
            sql = "INSERT INTO jobs(run_at, docker_image, status, " \
                  "env_vars, callback) VALUES('%s', '%s', '%s', '%s', '%s')" %(
                      stime.strftime('%Y-%m-%d %H:%M:%S'), docker_image,
                      STATUS_SCHEDULED, env_vars, callback)

        logging.debug("Saving job: %s", sql)
        cursor.execute(sql)

    except Exception as e:
        logging.critical("Error when saving job into DB: %s", str(e))
        return -1


    # return the newly job generated id
    cursor.execute("SELECT last_insert_id()")
    job_id = cursor.fetchone()
    return job_id[0]



@app.route('/v1/jobs', methods=['POST'])
def schedule_job():
    """

    This function handles the POST /v1/jobs request (a new job must
    be created and scheduled).

    It reads the JSON sent, makes sure it's well formed, parse its
    data and store it on a database.

    A 400 code can be returned if the JSON is not well formed, the
    specified schedule date/time is not in the future or if there's
    any mandatory field missing.

    This is an example JSON:

         {
               "docker_image": "xx",
               "date_time": "YYYY-MM-DD HH:MM:SS",
               "callback": "http://mycallback.com?xx",
               "env-variables": {
                       "env1": "e1",
                       "env2": "e2"
               }
         }

    date_time must be specified in the UTC timezone. env-variables and
    callback parameters are optional.

    If everything is OK, a 201 is returned with a JSON confirming the
    job data and with its job-id.

    """

    # Make sure a well formed json was posted
    try:
        json = request.get_json()
    except:
        return jsonify({"Error": "Bad request. Make sure that the JSON " \
                       "posted is well formed."}), 400

    # Return an error if there is any mandatory field missing
    if not(json.has_key("docker_image") and json.has_key("datetime")):
        return jsonify({"Error": "docker_image or datetime is missing"}), 400

    # Verify if time format is ok and stores in into a time-tuple format
    try:
        stime = datetime.strptime(json["datetime"], "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return jsonify({"Error": "Date format must be yyyy-mm-dd hh:mm:ss"}), \
               400

    # Ensure time is in the future
    if stime < datetime.now():
        return jsonify({"Error": "Date/time must be in the future"}), 400

    # Get callback if it was sent
    callback = json.get("callback", "")

    # Get the env_vars is it was sent
    env_vars = None
    if json.has_key("env_vars"):
        if type(json["env_vars"]) is dict:
            env_vars = json["env_vars"]

    # Convert the env vars to a notation format to be accepted
    # by docker("-e 'X1=v1' -e 'X2=v2' etc)
    env_vars = build_env_vars_docker_format(env_vars)

    # Save the job in the database and get its unique job id
    job_id = save_job_schedule(db_conn, json["docker_image"], stime,
                               callback, env_vars)
    if job_id == -1:
        return make_response(
            jsonify({'error': 'Something went wrong when attempting ' \
                    'to save data into db'}), 500)

    # Schedule the spot instance with AWS and update the db
    # with the parameters gotten.
    [req_id, req_state, req_status_code] = \
        aws.create_spot_instance(config["aws"], job_id, stime,
                                 json["docker_image"], env_vars)
    db.update_db(db_conn, job_id, req_id=req_id, req_state=req_state,
                 req_status_code=req_status_code)

    # Returns a json with the accepted json data and the job-id appended
    json["job_id"] = job_id
    return jsonify(json), 201



@app.route('/v1/jobs', methods=['GET'])
def get_list():
    """
    Handles the GET /v1/jobs request (where the service should respond
    with a list of all jobs.

    Currently all jobs scheduled over the last 24h are returned. An obvious
    extension to this call is to allow parametrization.
    """

    # From the database, get all jobs scheduled in the last 24h
    # and store them on a dict
    cursor = db_conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("SELECT * FROM jobs WHERE run_at > NOW() - INTERVAL 1 DAY")

    # Respond with a json containing all the data
    return jsonify(scheduled_jobs=cursor.fetchall()), 200



@app.route('/v1/jobs/<job_id>')
def get_status(job_id):
    """
    Handles the GET /v1/jobs/<job-id> request (asking for a specific
    job status).

    See module docstring for an example request and response and
    fields meaning.
    """

    # Get data from the DB about the specific job
    cursor = db_conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("SELECT * FROM jobs WHERE id = %s" % job_id)

    # 404 if no such job was found
    if cursor.rowcount == 0:
        return make_response(jsonify({
            'error': 'No job with such id was found'}), 404)

    # Otherise, return the job data
    return jsonify(job=cursor.fetchone())



@app.route('/v1/jobs/<job_id>', methods=['PUT'])
def update_job(job_id):
    """
    Handles the PUT /v1/jobs/<job-id> request (where a specific job
    must be updated). Currently, only callback is supported.
    job status).

    A JSON must be sent along with the request specifying the new
    callback to be used.
    """

    # Make sure a well formed json was posted
    try:
        json = request.get_json()
    except:
        return jsonify({"Error": \
           "Bad request. Make sure that the JSON posted is well formed."}), 400

    # Job exists and it's still time to change? (it's not already
    # running or it's done?)
    cursor = db_conn.cursor()
    cursor.execute("SELECT * FROM jobs WHERE id = %s AND status='%s'" \
        %(job_id, STATUS_SCHEDULED))
    if cursor.rowcount == 0:
        return make_response(jsonify({'error': \
            'No job with such id was found or the job is already ' \
            'running or done'}), 404)

    # Make sure the JSON contains a callback field
    elif not json.has_key("callback"):
        return make_response(jsonify({'error': \
            'No callback was specified.'}), 400)

    # Try to update the database with the new callback endpoint
    try:
        db.update_db(db_conn, job_id, callback=json["callback"])
        return make_response(jsonify({'Success': \
            'Callback function updated to %s' % json["callback"]}), 200)

    except Exception as e:
        return make_response(
            jsonify({'Error': \
                'Something went wrong when updating DB - %s' % str(e)}), 500)




@app.route('/v1/notifications/<job_id>', methods=['PUT'])
def process_notification(job_id):
    """
    Handles the PUT /v1/notifications/<job-id>?status=xxx request.
    This enpoint is used by the spot instances to notify about some
    important event (notably the container has started or finished).

    The database is updated so that future GET /v1/jobs/<job-id> requests
    get the latest information.

    If a "finished" status has been sent, the user defined callback endpoint
    (if any) for that job is called and the spot instance is terminated. In
    addition, a note is stored in the "notes" column stating if
    the callback ran ok or not.

    """


    # Get the status sent
    status = request.args.get('status')
    if status == None:
        return make_response(jsonify({'Error': \
            'Missing status= parameter'}), 400)

    # Try to update the DB with the status
    try:
        db.update_db(db_conn, job_id, status=status)

        # If status is "finished', then execute the user defined callback
        # for this job and terminate the spot instance.
        if status == "finished":
            logging.info("Executing job %s user callback function: %s", \
                         job_id, callback_function(job_id))
            call_callback(job_id)

            instance_id = db.job_db_data(db_conn, job_id, "instance_id")
            logging.info("Terminating job %s spot instance %s", \
                         job_id, instance_id)
            aws.terminate_instance(instance_id)

            logging.info("Marking job %s as done", job_id)
            db.update_db(db_conn, job_id, status='%s' % STATUS_DONE)

        return make_response(jsonify({'Success': \
            'Notification has been processed, status updated to %s' % \
            status}), 200)

    except Exception as e:
        return make_response(jsonify({'Error': \
            'Something went wrong when updating DB - %s' % str(e)}), 500)





@app.errorhandler(404)
def not_found():
    """
    404 handler
    """
    return make_response(jsonify({'error': 'Not found'}), 404)



def callback_function(job_id):
    """
    Gets the callback function defined by the used for the specified
    job and returns it.

    Args:
        job_id: the job id

    Returns:
        The callback endpoint for the job id.

    """
    [callback] = db.job_db_data(db_conn, job_id, "callback")
    return callback




# Status
#    submitted | running | done
#
# States:
#    open(request state)
#      pending-evaluation(status code)
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
    This function is called by apscheduler every 60s (configurable).

    For each job that is ready to run and still needs to be processed
    (status is not 'done'), this function polls AWS to get
    the latest information about the corresponding spot instance request
    (its state, status code and instance_id, if any) and updates the local
    database.

    If the request state is marked as closed (and not due to an application
    ask), this means the spot instance was interrupted for some reason. In
    this case, a new spot instance is scheduled to be run one minute
    from now.
    """


    # Get all jobs that are ready to be processed (scheduled
    # for a time in the past and has not been finished yet)
    cursor = db_conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("SELECT * FROM jobs WHERE run_at <= NOW() AND " \
        "status <> '%s' AND (req_state='active' OR " \
        "req_state='open')" % STATUS_DONE)
    rows = cursor.fetchall()

    # For each job
    for row in rows:
        logging.info("Polling job %s status with AWS", row['id'])
        logging.debug("DB row: %s", row)

        # Asks AWS about the latest information about the job
        # spot instance request and save into the local database
        # If the instance was running and has been terminated, run
        # it again.
        job_id = row['id']
        [aws_req_state, aws_req_status_code, aws_instance_id] = \
            aws.get_aws_req_status(row['req_id'])

        if aws_req_state == 'open':
            db.update_db(db_conn, job_id, req_state=aws_req_state,
                         req_status_code=aws_req_status_code)
        elif aws_req_state != 'closed':
            db.update_db(db_conn, job_id, req_state=aws_req_state,
                         req_status_code=aws_req_status_code,
                         instance_id=aws_instance_id)
        elif aws_req_state == 'closed':
            rerun(job_id)



def rerun(job_id):
    """
    Schedules again a job with AWS. To be used when it had been started
    but its instance was terminated before the job completed.
    The local database is also updated with the new status and schedule
    data (that is now + 1 minute).

    Args:
        job_id: The job id.

    """

    # Get all the necessary data to schedule a new spot instance
    # (container image, env vars)
    [docker_image, env_vars] = db.job_db_data(db_conn, job_id,
                                              "docker_image", "env_vars")

    # Re-schedule the spot instance to run 1 minute from now
    stime = datetime.now()+timedelta(minutes=1)
    [req_id, req_state, req_status_code] = \
        aws.create_spot_instance(config["aws"], job_id, stime,
                                 docker_image, env_vars)

    # Updates the database with the new status and scheduled time
    db.update_db(db_conn, job_id, req_id=req_id, req_state=req_state, \
                 req_status_code=req_status_code, status="%s" % \
                 STATUS_RE_SCHEDULED, instance_id="", run_at=stime)



def call_callback(job_id):
    """
    Calls the user define callback endpoint for the specified job.
    The notes job column is updated with the success/error message.

    Args:
        job_id: the job id.

    """

    # Get the callback endpoint fot the job
    url = callback_function(job_id)

    try:
        # Try to call it
        f = urllib2.urlopen(url)
        f.close()


    except URLError as e:
        logging.info("Error when calling back job %s callback function(%s)", \
                     job_id, url)
        db.update_db(db_conn, job_id, notes="Tried to callback %s but" \
                     " seems like an invalid url: %s" %(url, e.reason))

    except Exception as e:
        db.update_db(db_conn, job_id, notes="Something went wrong when" \
                     " trying to callback %s: %s" %(url, e.message))


    else:
        logging.info("Job %s callback function(%s) called " \
                     "successfully", job_id, url)
        db.update_db(db_conn, job_id,
                     notes="Called back %s sucessfully at %s" %
                     (url, datetime.now()))


def build_env_vars_docker_format(env_vars):
    """
    Get the env vars specified by a dict and returns an equivalent
    docker command line parameters.

    Args:
       env_vars: dict with the vars

    Returns:
       A string like "-e 'VAR1'='val1' -e 'VAR2'='val2'
    """

    # Collect the env vars if any was specified
    env_vars_parameter = ""
    if not env_vars is None:
        for k, v in env_vars.iteritems():
            env_vars_parameter = env_vars_parameter + "-e '%s=%s' " %(k, v)


    return env_vars_parameter



def setup_logging(config):
    """
    COnfigures the logging module with the specified configutation.

    Args:
        config: dict with the parameters. The only one supported for now
           is level.
    """

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
        config["file"], maxBytes=config["max-bytes"],
        backupCount=config["backup-count"])
    formatter = logging.Formatter("%(asctime)s - %(name)s - " \
        "%(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)





if __name__ == '__main__':

    STATUS_SCHEDULED = "scheduled"
    STATUS_RE_SCHEDULED = "re-scheduled"
    STATUS_DONE = "done"

    CONFIG_FILE = "scheduler.yaml"
    config = read_config(CONFIG_FILE)
    logging.debug("Config read: %s", config)

    setup_logging(config["log"])
    db_conn = db.open_connection(config["db"])

    spot_sg_id = aws.create_spot_security_group(config["aws"]["sg-name"])

    # pylint: disable=no-value-for-parameter
    scheduler = BackgroundScheduler()
    scheduler.add_job(check_jobs, 'interval',
                      seconds=config["app"]["polling-interval"])
    scheduler.start()

    app.run(host='0.0.0.0', port=80)






