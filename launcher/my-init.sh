#!bin/bash
# 
# Scheduler deployment script.
# In a nutshell, it ensures docker is installed and running and then runs 2 containers (one with a mysql
# server, another with the scheduler server).
#

touch /tmp/start
yum update -y
yum install docker -y
service docker start
docker run --name scheduler-db  -v /var/lib/mysql:/var/lib/mysql -e MYSQL_ROOT_PASSWORD=yagg27 -d enyamada/scheduler-db:1.0
sleep 60
docker run --name scheduler-web  -e LOG_LEVEL=debug  -v /var/log/:/var/log/ -e AWS_ACCESS_KEY_ID='%s' -e AWS_SECRET_ACCESS_KEY='%s' -d -p 80:80 --link scheduler-db:mysql enyamada/scheduler-web:1.2
touch /tmp/fin

