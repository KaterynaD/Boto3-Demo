#!/bin/bash
#Web instance start script:
#1. Install needed software
#2. Copies and makes executable scripts from S3 buckets
#3.Sends a message, the instance is ready
#4.Runs a Web report available for http access

yum install gcc python-setuptools python-devel postgresql-devel  -y
easy_install psycopg2
pip install boto3
pip install cherrypy
yum update -y
aws s3 cp --recursive s3://kdweb/ /home/ec2-user/
cd /home/ec2-user/
chown -R ec2-user /home/ec2-user/
chmod u+x WebReport.py
chmod u+x  SendToQueue.py
./SendToQueue.py WebProjectResources.yml "WebInstance is ready"
./WebReport.py
