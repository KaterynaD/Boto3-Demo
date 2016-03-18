#!/bin/bash
#Creates RedshiftAll security group in VPC, Redshift in a private subnet, sends a message to the queue when ready
./RedshiftUtility.py RedshiftAll
./RedshiftUtility.py CreateInVPCPrivate
./SendToQueue.py ProjectResources.yml "Redshift is ready"
