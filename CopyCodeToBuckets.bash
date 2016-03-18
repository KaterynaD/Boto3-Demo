#!/bin/bash
#Copies scripts to S3 buckets for instances scripts
#kdsupportdwdbloadscripts - AppInstance scripts
#aws s3 rm s3://kdsupportdwdbloadscripts/ --recursive
aws s3api create-bucket --bucket kdsupportdwdbloadscripts --region us-west-2
aws s3 cp DataLoadResources.yml s3://kdsupportdwdbloadscripts/DataLoadResources.yml
aws s3 cp RedshiftUtility.py  s3://kdsupportdwdbloadscripts/RedshiftUtility.py
aws s3 cp SQSUtility.py  s3://kdsupportdwdbloadscripts/SQSUtility.py
aws s3 cp DataLoad.py  s3://kdsupportdwdbloadscripts/DataLoad.py
aws s3 cp HistoricalDataLoad.py s3://kdsupportdwdbloadscripts/HistoricalDataLoad.py
aws s3 cp ProcessPeriodicData.py s3://kdsupportdwdbloadscripts/ProcessPeriodicData.py
aws s3 cp RemoveData.bash s3://kdsupportdwdbloadscripts/RemoveData.bash
aws s3 cp SendToQueue.py s3://kdsupportdwdbloadscripts/SendToQueue.py
aws s3 cp DeleteDataFromBuckets.py s3://kdsupportdwdbloadscripts/DeleteDataFromBuckets.py
aws s3 cp NewDataForLoad.json s3://kdsupportdwdbloadscripts/NewDataForLoad.json
aws s3 cp StagingArea.json s3://kdsupportdwdbloadscripts/StagingArea.json
aws s3 cp SupportDW.json s3://kdsupportdwdbloadscripts/SupportDW.json
aws s3 cp SupportDW_NewData.json s3://kdsupportdwdbloadscripts/SupportDW_NewData.json
aws s3 cp sql s3://kdsupportdwdbloadscripts/sql --recursive

#kdweb - WebInstance scripts
#aws s3 rm s3://kdweb/ --recursive
aws s3api create-bucket --bucket kdweb --region us-west-2
aws s3 cp WebProjectResources.yml s3://kdweb/WebProjectResources.yml
aws s3 cp RedshiftUtility.py s3://kdweb/RedshiftUtility.py
aws s3 cp SQSUtility.py  s3://kdweb/SQSUtility.py
aws s3 cp HTML.py s3://kdweb/HTML.py
aws s3 cp WebReport.py s3://kdweb/WebReport.py
aws s3 cp SendToQueue.py s3://kdweb/SendToQueue.py
aws s3 cp report-sql s3://kdweb/report-sql --recursive

