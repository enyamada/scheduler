# scheduler

This is my solution to the proposed jobs scheduler problem.


## Launching the server

This command will create a new EC2 instance running a scheduler server:
```
sudo docker run -it -e AWS_ACCESS_KEY_ID=XXX -e AWS_KEY_NAME=my-key-name -e AWS_SECRET_ACCESS_KEY=YYYY  enyamada/scheduler-launcher
```

All the necessary AWS resources (including the spot instance requests to be launched by the scheduler) will use the mentioned credentials and key name. The region to be used will be us-west-2.

The script will output the new created server name along with some example requests using curl. A more detailed API documentation
can be found in the [module docstrings] (https://github.com/enyamada/scheduler/blob/master/web/scheduler.py)


## A few implementation details

* Written in python + boto3. [Flask](http://flask.pocoo.org/) was used to ease the REST API implementation.
* When a new job creation is requested, the service essentially
  * Issues a spot instance request with the provided data
  * The "UserData" field is filled in with a shell script that calls the container with the provided parameters. In addition, that script also calls a REST API (`PUT /v1/notifications/job-id?status=xxx`) that will notify us when the container is about to run and right after it ends. 
  * Save the data in a database
* A status request essentially reads the database and then spits out a JSON.
* Every 60s (configurable), the server, for each job ready to run, asks AWS if any update has occurred (for example, if the spot instance has been created). If so, the server updates the database so that eventual job status requests will show the latest data.
  * In particular, if the instance was running but was terminated for any reason before the container ended the job will be re-scheduled to run immediately with the same original parameters and the process will start over.
* When the container ends, the server will be notified by the instance's user data script, that will then call the user defined callback (if any), update the status accordingly and then terminate the instance.


## Some TODO things

* No authentication was implemented. 
* The user provided API parameters are not being carefully examined.
* 
