#!/usr/bin/python
import RedshiftUtility
import VPCUtility
import SNSUtility
import SQSUtility
import LBUtility
import ASUtility
import IAMUtility
import yaml
import sys

"""
	The module deletes VPC with its EC2 instances, subnet groups, a gateway, route, route table,
	Redshift cluster, load balancer, autoscaling group, launch configuration, IAM roles, progile,
	policy, SQS queue and SNS topic
"""

#0.Create SNS Topic
MyTopic=SNSUtility.SNSUtility(resource="ProjectResources.yml")
if not MyTopic.TopicArn:
    print "Creating a new Topic"
    MyTopic.create_topic()
    print "New topic arn: %s" %MyTopic
    print "New Subscription ID: %s" %MyTopic.TopicSubscribeEmail("drogaieva@gmail.com")
else:
    print "Topic exists with: %s" %MyTopic
#0.Create SQS Queue
MyQueue=SQSUtility.SQSUtility(resource="ProjectResources.yml")
if not MyQueue.QueueUrl:
    print "Creating a new Queue"
    MyQueue.create_queue()
    print "New Queue url %s" %MyQueue
else:
    print "Queue exists with: %s" %MyQueue



#1.Delete Autoscaling group
AutoScalToDelete=ASUtility.ASUtility("ProjectResources.yml")
if AutoScalToDelete.LCARN and AutoScalToDelete.ASARN:
    print "Deleting Autoscaling and Launch Group"
    AutoScalToDelete.delete_as()
    AutoScalToDelete.delete_lc()

#2.Delete Load Balancer
LBToDelete=LBUtility.LBUtility("ProjectResources.yml")
if LBToDelete.DNSName:
    print "Deleting Load Balancer"
    LBToDelete.delete_lb()
    
    
#3.Delete Cluster
print "Deleting cluster and subgroup. It can take around 10 min or more..."
Cluster=RedshiftUtility.RedshiftUtility("ProjectResources.yml")
Cluster.DeleteCluster()
Cluster.WaitForDeletion()
Cluster.DeleteClusterSubgroup()
MyTopic.TopicPublish("Redshift was deleted","Done") 
    
#4.Delete VPC and all its components
MyVPC=VPCUtility.VPCUtility("ProjectResources.yml")
if  MyVPC.Vpc:
    print "Deleting VPC..."
    MyVPC.delete_vpc()

#5.Roles, instance profile and policies 
with open("ProjectResources.yml", "r") as f:
    res = yaml.load(f)
print "Deleting roles, instance profile and policies..."
role=IAMUtility.RoleUtility(res["IAM"]["InstanceRole"]["Name"])
if role.RoleArn:
    role.delete_role()
role=IAMUtility.RoleUtility(res["IAM"]["TempBucketAccessRole"]["Name"])
if role.RoleArn:
    role.delete_role()
policy=IAMUtility.PolicyUtility(res["IAM"]["AccessPolicy"]["Name"])
if policy.PolicyArn:
    policy.delete_policy()
instance_profile=IAMUtility.InstanceProfileUtility(res["IAM"]["InstanceRole"]["Name"])
if instance_profile.ProfileArn:
    instance_profile.delete_profile()
    
#6.Delete SQS Queue
print "Deleting Queue..."
MyQueue.QueueDelete()

#7.Delete topic
print "Deleting topic..."
MyTopic.TopicPublish("Done with the project","Currently deleting the topic... The End")
MyTopic.TopicDelete()

