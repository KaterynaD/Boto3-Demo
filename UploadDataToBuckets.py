#!/usr/bin/python
import os.path
import yaml
import boto3
from botocore.client import ClientError
import glob
import sys
"""
	UploadDataToBuckets.py - 03.14.16 Kate Drogaieva
    This module uploads a support application data set into S3 buckets
    The files are expected in the directories configured in a YAML resource file
    as well as the correspondent bucket names for historical data 
    and new (periodically extracted from an application)
Region: "us-west-2"
HistoricalDataFiles:
  Bucket: "kdsupportdata"
  LocalDir: "data/HistoricalData"
  FilesFilter: "*.csv"
NewDataFiles:
  Bucket: "kdsupportdatanew"
  LocalDir: "data/NewData"
  FilesFilter: "*.csv"
  
  The default resource file name is ProjectResources.yml
"""
#==============================================================================================================================#
try:
    ResourceFile=sys.argv[1]
except:
    ResourceFile="ProjectResources.yml"
print "Support data warehouse data are uploaded into S3 buckets according to the configuration parameters in %s." %ResourceFile
with open(ResourceFile, "r") as f:
    res = yaml.load(f)

s3 = boto3.resource("s3")

#Historical data

try:
    print "Checking if the bucket %s alreday exists..." %res["HistoricalDataFiles"]["Bucket"]
    s3.meta.client.head_bucket(Bucket=res["HistoricalDataFiles"]["Bucket"])
    s3.Bucket(res["HistoricalDataFiles"]["Bucket"]).objects.delete()
    print "The bucket was cleaned up"
except ClientError as e:
    error_code = int(e.response["Error"]["Code"])
    if error_code == 404:
        print "Creating a new bucket %s" %res["HistoricalDataFiles"]["Bucket"]
        s3.create_bucket(Bucket=res["HistoricalDataFiles"]["Bucket"], CreateBucketConfiguration={"LocationConstraint": res["Region"]})
#and upload historical data
data_exists_flag=False
for file in glob.glob(os.path.join(res["HistoricalDataFiles"]["LocalDir"],res["HistoricalDataFiles"]["FilesFilter"])):
        print "Upload..%s" %file
        s3.Bucket(res["HistoricalDataFiles"]["Bucket"]).upload_file(file, os.path.basename(file))
        data_exists_flag=True
if not(data_exists_flag):
    sys.exit("No historical data found in %s. Stop the program" %res["Files"]["LocalDir"])

#Periodic data
    
try:
    print "Checking if the bucket %s alreday exists, delete the data in it..." %res["NewDataFiles"]["Bucket"]
    s3.meta.client.head_bucket(Bucket=res["NewDataFiles"]["Bucket"])
    s3.Bucket(res["NewDataFiles"]["Bucket"]).objects.delete()
    print "The bucket was cleaned up"
except ClientError as e:
    error_code = int(e.response["Error"]["Code"])
    if error_code == 404:
        print "Creating a new bucket %s" %res["NewDataFiles"]["Bucket"]
        s3.create_bucket(Bucket=res["NewDataFiles"]["Bucket"], CreateBucketConfiguration={"LocationConstraint": res["Region"]})
#and upload new data
data_exists_flag=False
for file in glob.glob(os.path.join(res["NewDataFiles"]["LocalDir"],res["NewDataFiles"]["FilesFilter"])):
        print "Upload...%s" %file
        s3.Bucket(res["NewDataFiles"]["Bucket"]).upload_file(file, os.path.basename(file))
        data_exists_flag=True
if not(data_exists_flag):
    sys.exit("No data found in %s. Stop the program" %res["NewDataFiles"]["LocalDir"])
