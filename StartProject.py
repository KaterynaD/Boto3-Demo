#!/usr/bin/python
import RedshiftUtility
import VPCUtility
import SNSUtility
import SQSUtility
import LBUtility
import ASUtility
import sys
import os
import subprocess
import time
"""
        The module creates SQS queue, SNS topic, VPC, subnets, a route, route table, gateway,
        NAT instance, Redshift cluster.
        Generates set of a Support department data warehouse source data, loads them in S3 buckets
        and then in the cluster and convert to a star schema.
        Creates minimum 2 EC2 instances to process the data in the cluster (App) and to produce HTML reports (Web)
        An autoscaling group, launch configuration and load balancer created in the application controls Web instances (1-3)
        IAM roles, profile and policy used in the application are also created on the fly
"""
#--------------------------------------------------------
def Step1():
    """
        Creating Topic
    """
    Topic=SNSUtility.SNSUtility(resource="ProjectResources.yml")
    if not Topic.TopicArn:
        print "Creating a new Topic"
        Topic.create_topic()
        print "New topic arn: %s" %Topic
        print "New Subscription ID: %s" %Topic.TopicSubscribeEmail("drogaieva@gmail.com")
    else:
        print "Topic exists with: %s" %Topic
    return Topic
#--------------------------------------------------------
def Step2():
    """
        Creating SQS Queue
    """
    Queue=SQSUtility.SQSUtility(resource="ProjectResources.yml")
    if not Queue.QueueUrl:
        print "Creating a new Queue"
        Queue.create_queue()
        print "New Queue url %s" %Queue
    else:
        print "Queue exists with: %s" %Queue
    return Queue
#--------------------------------------------------------
def Step3():
    """
        Creating VPC and NAT instance
    """
    VPC=VPCUtility.VPCUtility("ProjectResources.yml")
    if not VPC.Vpc:
        print "Creating %s VPC " %VPC.VpcName
        VPC.create_vpc()
        print "VPC %s created with Id: %s" %(VPC.VpcName,VPC.Vpc.vpc_id)
    else:
        print "VPC %s already exists" %VPC.VpcName
    #Create NAT instance
    NatId=VPC.create_nat_instance()
    print "NAT instance is starting with Id: %s" %NatId
    return VPC
#--------------------------------------------------------
def Step4(VPC):
    """
        Creating Redshift
    """
    print "Creating Redshift cluster. It takes several minutes..."
    Cluster=RedshiftUtility.RedshiftUtility("ProjectResources.yml")
    SubnetId=VPC.GetPrivateSubnets()[0]
    SecurityGroupId=VPC.create_security_group("RedshiftAll")
    print "Security group for Redshift Cluster was created with id: %s" %SecurityGroupId
    Cluster.CreateCluster([SubnetId],[SecurityGroupId])
    Cluster.WaitForCreation()
    return Cluster
#--------------------------------------------------------
def Step5(VPC):
    """
        Creating App instance and load cluster
    """
    print "Creating App Instance and load data warehouse"
    SubnetId=VPC.GetPrivateSubnets()[0]
    AppId=VPC.create_instance("App",SubnetId)
    print "AppInstance is starting with Id: %s" %AppId
    AppInstance=VPC.GetInstance("App")
    return
#--------------------------------------------------------
def Step6(VPC):
    """
        Creating Load Balancer
    """
    LB=LBUtility.LBUtility("ProjectResources.yml")
    if LB.DNSName:
        print "LB exists with DNSName: %s" %LB
    else:
        print "Creating new LB..."
        SubnetIDs=VPC.GetPublicSubnets()
        SecurityGroup=""
        try:
            SecurityGroup=VPC.GetSecurityGroupId("WebGroup")[0]
        except (ValueError,IndexError):
            pass
        if not(SecurityGroup):
            SecurityGroup=VPC.create_security_group("WebGroup")
        if not(SubnetIDs):
            sys.exit("Cannot create a new LB because no Subnets")
        if not(SecurityGroup):
            sys.exit("Cannot create a new LB because no SecurityGroups")
        LB.create_lb([SecurityGroup],SubnetIDs)
        print "Load Balancer is ready - %s" %LB
    return LB
#--------------------------------------------------------
def Step7(VPC,LB,Topic):
    """
        Creating Autoscaling
    """
    AS=ASUtility.ASUtility("ProjectResources.yml")
    if AS.ASARN:
        print "Autoscaling exists with %s" %AS.ASARN
    else:
        #Create a SecurityGroup
        SecurityGroupId=""
        try:
            SecurityGroupId=VPC.GetSecurityGroupId("WebGroup")[0]
        except (ValueError,IndexError):
            pass
        if not(SecurityGroupId):
            SecurityGroupId=VPC.create_security_group("WebGroup")
        PublicSubnets=",".join(VPC.GetPublicSubnets())
        AvailabilityZones=[]
        for Subnet in VPC.Vpc.subnets.all():
            if Subnet.id  in PublicSubnets:
                AvailabilityZones.append(Subnet.availability_zone)
        print "Creating Launch Configuration and Autoscaling group..."
        AS.create(
        InstanceName="Web",
        SecurityGroup=SecurityGroupId,
        AvailabilityZones=AvailabilityZones,
        LBName=LB.LBName,
        VPCSubnetIds=PublicSubnets,
        TopicARN=Topic.TopicArn
                )
        AS.get_as()
        print "Autoscaling was created with: %s" %AS.ASARN
    return AS
#--------------------------------------------------------
def SimpleStep7(VPC):
    """
        Creating Web Instance
    """
    print "Creating Web Instance"
    SubnetId=VPC.GetPublicSubnets()[0]
    WebId=VPC.create_instance("Web",SubnetId)
    print "WebInstance is starting running with Id: %s" %WebId
    WebInstance=VPC.GetInstance("Web")
    return WebInstance
#=========================================================
resources_filepath = os.path.abspath(os.path.expanduser("ProjectResources.yml"))
# Check if the resource file exists.
if not os.path.exists(resources_filepath):
    sys.exit("ProjectResources.yml file is required to run the application")
#1.Create SNS Topic
MyTopic=Step1()
#2.Create SQS Queue
MyQueue=Step2()
#3.Create VPC
MyVPC=Step3()
#4.Create Redshift and prepare (extract or generate) data for the load, create IAM roles for the instances
#load buckets with scripts for the instances in parallel to save time
#MyCluster=Step4(MyVPC)
#Call external program instead of Step4 in async mode and wait till all of them are done
subprocess.Popen(["./CreateRedshift.bash"])
subprocess.Popen(["./PrepareData.bash"])
#These 2 scripts does not take a lot of time. We do not wait for completion
subprocess.Popen(["./CopyCodeToBuckets.bash"])
subprocess.Popen(["./IAMUtility.py","ProjectResources.yml"])

#Wait till Cluster is ready and Data are ready

ReadyMessages=MyQueue.MessagesWait(["Redshift is ready","Data are ready"],30*60)

if "Redshift is ready" in ReadyMessages["Received"]:
    print "Cluster is now available"
    MyTopic.TopicPublish("Redshift is ready","Cluster is now available")
else:
    MyTopic.TopicPublish("Redshift is not available","")
    sys.exit("Cluster is NOT available...")

if "Data are ready" in ReadyMessages["Received"]:
    print "Data is now available for the load"
    MyTopic.TopicPublish("Data are ready","Data is now available for the load")
else:
    MyTopic.TopicPublish("Data are not available","")
    sys.exit("Data are not available...")


#5.Create AppInstance
Step5(MyVPC)
# Wait till all scripts are complete and AppInstance will send us a confirmation code via SQS Queue or 30 min
AppInstanceReady=MyQueue.MessageWait("AppInstance is ready",30*60)

if not AppInstanceReady:
    MyTopic.TopicPublish("App Instance Issue","App Instance is not ready in 30 min")
    sys.exit("App Instance is not ready")

print "App Instance is redy"
MyTopic.TopicPublish("App Instance is redy","Done with App instance...")
#6.Create Load Balancer
MyLB=Step6(MyVPC)
#7. Create autoscaling group
Step7(MyVPC,MyLB,MyTopic)
#Wait
WebInstanceReady=MyQueue.MessageWait("WebInstance is ready",30*60)

LBisReady=False
i=1
while i<10 and not(LBisReady):
    time.sleep(60)
    Status=MyLB.getStatus()
    print "Load Balancer Status: %s" %Status
    if Status=="InService":
        LBisReady=True
    i+=1
if WebInstanceReady and LBisReady:
    Link=MyLB.DNSName+":"+str(MyLB.getListener()["LoadBalancerPort"])
    Href="<a href='%s'>%s</a>" %(Link,Link)
    print "The data are ready: "
    print Link
    MyTopic.TopicPublish("Web Access is ready","The data are ready: %s" %Href)
else:
    print "Web Instances or LB are not ready"
    MyTopic.TopicPublish("Web Instanes or Load Balancer issue","Web Instanes or Load Balancer issue")
#7. Or Create WebInstance without a load balancer etc
#WebInstance=SimpleStep7(MyVPC)
#Wait
#WebInstanceReady=MessageWait(MyQueue,"WebInstance is ready",30*60)
#if not(WebInstance):
#   MyTopic.TopicPublish("Web Instance Issue","Web Instance is not ready in 30 min")
#   sys.exit("Web Instance is not ready")
#print "Web Instance is ready"
#print "Try it: %s:8080" %WebInstance.public_ip_address
#MyTopic.TopicPublish("Web Instance is ready","Try it: %s:8080" %WebInstance.public_ip_address, "Web Access is ready")

