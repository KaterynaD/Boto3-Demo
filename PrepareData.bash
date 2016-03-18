#!/bin/bash
#Generate data for a Support Department data warehouse, uploads them to S3 buckets and sends a message when ready
./GenerateData.py DataResources.yml
./UploadDataToBuckets.py DataResources.yml
./SendToQueue.py DataResources.yml "Data are ready"
