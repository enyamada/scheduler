FROM centos:7
MAINTAINER "Edson Yamada" <enyamada@gmail.com>

RUN rpm -Uvh http://download.fedoraproject.org/pub/epel/7/x86_64/e/epel-release-7-5.noarch.rpm

RUN yum install -y \
          gcc \
          mysql-devel \
          MySQL-python \
          python-pip 

RUN pip install  \
          apscheduler==3.0.5 \        
          boto3 \
          Flask \
          pyyaml

EXPOSE 80

WORKDIR /root
RUN echo "cache-bust"
ADD aws/* /root/.aws/
ADD scheduler.yaml scheduler.yaml
ADD *.py /root/
CMD python scheduler.py
