#!/bin/bash
#App instance start script:
#1. Install needed software
#2. Copies and makes executable scripts from S3 buckets
#3. Runs scripts to upload data into Redshift and create a star schema
#It simulates one-time historical data load
#and periodic new data uploads
#4.Removes data from S3 buckets
#5.Sends a message when ready
yum install gcc python-setuptools python-devel postgresql-devel  -y
easy_install psycopg2
pip install boto3
yum update -y
aws s3 cp --recursive s3://kdsupportdwdbloadscripts/ /home/ec2-user/
cd /home/ec2-user/
chown -R ec2-user /home/ec2-user/
chmod u+x HistoricalDataLoad.py
chmod u+x ProcessPeriodicData.py
chmod u+x SendToQueue.py
chmod u+x RemoveData.bash
chmod u+x DeleteDataFromBuckets.py
./HistoricalDataLoad.py DataLoadResources.yml
./ProcessPeriodicData.py DataLoadResources.yml
./RemoveData.bash
./SendToQueue.py DataLoadResources.yml "AppInstance is ready"
