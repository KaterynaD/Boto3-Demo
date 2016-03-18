#!/bin/bash
#Deletes data and buckets with a Support department data warehouse and sends a message when ready
./DeleteDataFromBuckets.py DataLoadResources.yml
./SendToQueue.py DataLoadResources.yml "Data were deleted"

