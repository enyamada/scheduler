FROM centos:7
MAINTAINER "Edson Yamada" <enyamada@gmail.com>

RUN rpm -Uvh http://download.fedoraproject.org/pub/epel/7/x86_64/e/epel-release-7-5.noarch.rpm

RUN yum install -y \
          gcc \
          python-pip 

RUN pip install  \
          boto3 


WORKDIR /root
ADD aws/* /root/.aws/
ADD *.py /root/
