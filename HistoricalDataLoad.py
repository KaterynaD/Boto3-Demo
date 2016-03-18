#!/usr/bin/python
import psycopg2
import yaml
import json
import RedshiftUtility
import sys
import DataLoad
import boto3
"""
HistoricalDataLoad.py - 03.14.16 Kate Drogaieva
This module connects to a Redshift cluster using temporary access credentials 
and loads historical data to a staging area first and then converts them to a star schema
"""
#==============================================================================================================================#
try:
    ResourceFile=sys.argv[1]
except:
    ResourceFile="ProjectResources.yml"

with open(ResourceFile, "r") as f:
    res = yaml.load(f)

print "Load data to the database in staging tables and transform to star schema"
print "Getting Redshift endpoint to connect..."
c=RedshiftUtility.RedshiftUtility(ResourceFile)
host=c.endpoint
if not(host):
    sys.exit("Redshift Cluster is not available. Stop the program")
#....................................................................................................................
print "Granting Temp access to copy data from buckets...."
#Temp access to copy data from buckets
client = boto3.client("sts")
response = client.assume_role(
RoleArn=res["Access"]["RoleArn"],
RoleSessionName="DataLoad"
)
Bucket_Access={}
Bucket_Access["AccessKeyId"]=response["Credentials"]["AccessKeyId"]
Bucket_Access["SecretAccessKey"]=response["Credentials"]["SecretAccessKey"]
Bucket_Access["SessionToken"]=response["Credentials"]["SessionToken"]
#....................................................................................................................
try:
    conn = psycopg2.connect(
    host=host,
    user=res["RedshiftCluster"]["MasterUsername"],
    port=res["RedshiftCluster"]["Port"],
    password=res["RedshiftCluster"]["MasterUserPassword"],
    dbname=res["RedshiftCluster"]["DBName"])
    print "Loading historical data in staging area tables..."
    with open(res["DWDescription"]["HistoricalData"]) as data_file:
        data = json.load(data_file)
    sa=DataLoad.DataLoad(data,conn,ResourceFile,Bucket_Access)
    print str(sa)
    sa.run("drop") #We can ignore delete statements because it may be a new database
    if sa.run("create")>0:
        sys.exit("Error creating staging area tables...")
    if sa.run("load")>0:
        sys.exit("Error load data in staging area tables")
    print "Transform staging area into dimensions and fact tables..."
    with open(res["DWDescription"]["DW"]) as data_file:
        data = json.load(data_file)
    dw=DataLoad.DataLoad(data,conn,ResourceFile,Bucket_Access)
    print str(dw)
    dw.run("drop") #We can ignore delete statements because it may be a new database
    if dw.run("create")>0:
        sys.exit("Error creating star schema...")
    if dw.run("load")>0:
        sys.exit("Error transform data in star schema...")
    print "Dropping historical data in the staging tables..."
    sa.run("drop")
except KeyError:
    print "Wrong Redshift connect configuration parameters"
except psycopg2.Error as e:
    print "Unable to connect to %s" %host
    print e.pgerror
    print e.diag.message_detail
finally:
    if conn:
        conn.close();
