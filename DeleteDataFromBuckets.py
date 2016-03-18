#!/usr/bin/python
import os.path
import yaml
import boto3
from botocore.client import ClientError
import glob
import sys
"""
	DeleteDataFromBuckets.py - 03.14.16 Kate Drogaieva
    This module deletes buckets with a support application data set
    The bucket names are in a YAML resource file
HistoricalDataFiles:
  Bucket: "kdsupportdata"
NewDataFiles:
  Bucket: "kdsupportdatanew"
  
  The default resource file name is ProjectResources.yml
"""
#==============================================================================================================================#
try:
    ResourceFile=sys.argv[1]
except:
    ResourceFile="ProjectResources.yml"
    
with open(ResourceFile, "r") as f:
    res = yaml.load(f)

s3 = boto3.resource("s3")

print "Deleting the bucket with the new data..."
s3.Bucket(res["NewDataFiles"]["Bucket"]).objects.delete()
s3.Bucket(res["NewDataFiles"]["Bucket"]).delete()

print "Deleting the bucket with the historical data..."
s3.Bucket(res["HistoricalDataFiles"]["Bucket"]).objects.delete()
s3.Bucket(res["HistoricalDataFiles"]["Bucket"]).delete()
