#!/usr/bin/python

import boto3
from botocore.client import ClientError
import yaml
import sys
import os.path
"""
ASUtility.py - 03.14.16 Kate Drogaieva
This module provides a class to create a Launch Configuration and AutoScaling group
"""
class ASUtility(object):
    """
    ASUtility class creates a Launch Configuration group, Auto Scaling group and a scaling policy (SimpleType)
    The configuration parameters must be in a YAML resource file
    AutoScaling:
      Name: "TestAS"
      MinSize: "1"
      MaxSize: "3"
      DesiredCapacity:  "2"
      DefaultCooldown:  "300"
      HealthCheckType: "ELB"
      HealthCheckGracePeriod: "300"
      MetricName: "CPUUtilization"
      Statistic: "Average"
      Period: "60"
      ThresholdScaleDown: "40"
      ThresholdScaleUp: "70"
      EvaluationPeriods: "2"
      Unit: "Percent"
      Namespace: "AWS/EC2
    """
#-----------------------------------------------------------------------------
    def __init__(self,resource="",ASName=""):
        """
            An Auto Scaling group can be init via a name (ASName) or a resource YAML file (resource)
            If you plan to create new components you need a resource file with configuraton parameters
            For an already created Auto Scaling and launch configuration groups you can use ASName only 
            to get component attributes
        """
        self.res = False
        if resource:
            try:
                with open(resource, "r") as f:
                    self.res = yaml.load(f)
                self.ASName = self.res["AutoScaling"]["Name"]
                self.client = boto3.client("autoscaling",self.res["Region"])
                try:
                    self.get_lc()
                    self.get_as()
                except ClientError:
                    self.LCARN=""
                    self.ASARN=""
            except KeyError or IOError:
                sys.exit("Wrong Auto Scaling parameters")
        elif ASName:
            self.ASName = ASName
            self.client = boto3.client("autoscaling")
            try:
                self.get_lc()
                self.get_as()
            except ClientError:
                self.LCARN=""
                self.ASARN=""
        else:
            raise ValueError("Please provide a resource file name or Auto Scaling name")
        return
#-----------------------------------------------------------------------------
    def __str__(self):
        return self.ASName
#-----------------------------------------------------------------------------
    def create(self,
               InstanceName,
               SecurityGroup,
               AvailabilityZones,
               LBName,
               VPCSubnetIds,
               TopicARN):
        """
            The function creates:
            - a Launch Configuration group according to InstanceName from
            YAML resource file in SecurityGroup (string)
            The default group name is ASNameLaunchConfig
            - an Auto Scaling group in provided AvailabilityZones (list), VPCSubnetIDs (comma separated list of Subnet IDs)
            and attaches a load balancer (LBName)
            - adds Name tags to the Auto Scaling group and instances
            - adds SimpleScaling Policy and Metric alarms
            - subscribes the Auto Scaling group to TopicARN (all possible events)
            The resource file is mandatory to get all configuration parameters
        """
        if not(self.res):
            raise ValueError("Please provide a resource file to create Auto Scaling")
        if not(self.LCARN):
            self.create_lc(InstanceName,SecurityGroup)
        if not(self.ASARN):
            self.create_as(AvailabilityZones,LBName,VPCSubnetIds)
        self.add_InstanceName(InstanceName)
        self.SimpleScalingPolicy()
        self.add_notification(TopicARN)
#-----------------------------------------------------------------------------
    def create_lc_from_TemplateInstance(self,TemplateInstanceId,SecurityGroup):
        """
            The function creates a Launch Configuration group from 
            an existing instance (TemplateInstanceId) in SecurityGroup (string)
            The default group name is ASNameLaunchConfig
        """
        response = self.client.create_launch_configuration(
        LaunchConfigurationName=self.ASName+"LaunchConfig",
        SecurityGroups=[SecurityGroup],
        InstanceId=TemplateInstanceId
        )
#-----------------------------------------------------------------------------
    def create_lc(self,InstanceName,SecurityGroup):
        """
            The function creates  a Launch Configuration group according to InstanceName from
            YAML resource file in SecurityGroup (string)
            The default group name is ASNameLaunchConfig
        """
        if not(self.res):
            raise ValueError("Please provide a resource file to create Launch Configuration Group")
        for Instance in self.res["VPC"]["Instance"]:
            if Instance["Name"]==InstanceName:
                Script=""
                try:
                    if Instance["UserData"]:
                        Script=open(Instance["UserData"], "r").read()
                except KeyError or IOError:
                    print "UserData script can not be open for instance %s" %InstanceName
                AssociatePublicIpAddress=False
                if Instance["AssociatePublicIpAddress"]=="True":
                    AssociatePublicIpAddress=True
                response = self.client.create_launch_configuration(
                LaunchConfigurationName=self.ASName+"LaunchConfig",
                ImageId=Instance["ImageId"],
                KeyName=Instance["KeyName"],
                SecurityGroups=[SecurityGroup],
                UserData=Script,
                InstanceType=Instance["InstanceType"],
                IamInstanceProfile=Instance["IamInstanceProfileName"],
                AssociatePublicIpAddress=AssociatePublicIpAddress)
#-----------------------------------------------------------------------------                                                                    )
    def create_as(self,AvailabilityZones,LBName,VPCSubnetIds):
        """
            The function creates an Auto Scaling group in provided AvailabilityZones (list), VPCSubnetIDs (comma separated list of Subnet IDs)
            and attaches a load balancer (LBName)
        """
        if not(self.res):
            raise ValueError("Please provide a resource file to create Auto Scaling Group")
        response = self.client.create_auto_scaling_group(
    AutoScalingGroupName=self.ASName,
    LaunchConfigurationName=self.ASName+"LaunchConfig",
    MinSize=int(self.res["AutoScaling"]["MinSize"]),
    MaxSize=int(self.res["AutoScaling"]["MaxSize"]),
    DesiredCapacity=int(self.res["AutoScaling"]["DesiredCapacity"]),
    DefaultCooldown=int(self.res["AutoScaling"]["DefaultCooldown"]),
    AvailabilityZones=AvailabilityZones,
    LoadBalancerNames=[LBName],
    HealthCheckType=self.res["AutoScaling"]["HealthCheckType"],
    HealthCheckGracePeriod=int(self.res["AutoScaling"]["HealthCheckGracePeriod"]),
    VPCZoneIdentifier=VPCSubnetIds
        )
#-----------------------------------------------------------------------------
    def add_InstanceName(self,InstanceName):
        """
            The function adds Name tags to the Auto Scaling group and instances
        """
        self.client.create_or_update_tags(
            Tags=[
            {
            "ResourceId": self.ASName,
            "ResourceType": "auto-scaling-group",
            "Key": "Name",
            "Value": InstanceName,
            "PropagateAtLaunch": True
            },
                ]
                                               )
#-----------------------------------------------------------------------------
    def add_notification(self,TopicARN):
        """
            The function subscribes the Auto Scaling group to TopicARN (all possible events)
        """
        self.client.put_notification_configuration(
        AutoScalingGroupName=self.ASName,
        TopicARN=TopicARN,
        NotificationTypes=[
        "autoscaling:EC2_INSTANCE_LAUNCH",
        "autoscaling:EC2_INSTANCE_LAUNCH_ERROR",
        "autoscaling:EC2_INSTANCE_TERMINATE",
        "autoscaling:EC2_INSTANCE_TERMINATE_ERROR",
        "autoscaling:TEST_NOTIFICATION"
        ]
        )
#-----------------------------------------------------------------------------
    def delete_lc(self):
        """
            The function deletes the Launch Configuration group
        """
        response = self.client.delete_launch_configuration(LaunchConfigurationName=self.ASName+"LaunchConfig")
#-----------------------------------------------------------------------------
    def delete_as(self):
        """
            The function deletes the Auto Scaling group
        """
        response = self.client.delete_auto_scaling_group(AutoScalingGroupName=self.ASName,ForceDelete=True)
#-----------------------------------------------------------------------------
    def get_lc(self):
        """
            The function returns the Launch Configuration ARN (LCARN)
            It's blank if ASNameLaunchConfig does not exist
        """
        response = self.client.describe_launch_configurations(LaunchConfigurationNames=[self.ASName+"LaunchConfig"])
        try:
            self.LCARN=response["LaunchConfigurations"][0]["LaunchConfigurationARN"]
        except:
            self.LCARN=""
        return response
#-----------------------------------------------------------------------------
    def get_as(self):
        """
            The function returns the Auto Scaling ARN (ASARN)
            It's blank if ASName does not exist
        """
        response=self.client.describe_auto_scaling_groups(AutoScalingGroupNames=[self.ASName])
        try:
            self.ASARN=response["AutoScalingGroups"][0]["AutoScalingGroupARN"]
        except:
            self.ASARN=""
        return response
#-----------------------------------------------------------------------------
    def SimpleScalingPolicy(self):
        """
            The function creates SimpleScaling policy and metric alarms 
            according to the configuration parameters from YAML resource file
        """
        if not(self.res):
            raise ValueError("Please provide a resource file to create Scaling policy")
        #ScaleUp
        response = self.client.put_scaling_policy(
        AutoScalingGroupName=self.ASName,
        PolicyName="ScaleUp",
        PolicyType="SimpleScaling",
        AdjustmentType="ChangeInCapacity",
        ScalingAdjustment=int(self.res["AutoScaling"]["MinSize"]),
        Cooldown=int(self.res["AutoScaling"]["DefaultCooldown"])
        )
        #............................................................
        ScaleUpPolicyARN=response["PolicyARN"]
        #ScaleDown
        response = self.client.put_scaling_policy(
        AutoScalingGroupName=self.ASName,
        PolicyName="ScaleDown",
        PolicyType="SimpleScaling",
        AdjustmentType="ChangeInCapacity",
        ScalingAdjustment=-int(self.res["AutoScaling"]["MinSize"]),
        Cooldown=int(self.res["AutoScaling"]["DefaultCooldown"])
        )
        ScaleDownPolicyARN=response["PolicyARN"]
        #............................................................
        #............................................................
        #CloudWatch Alerts
        #Alarm ScaleUp
        client = boto3.client("cloudwatch",self.res["Region"])
        #............................................................
        response = client.put_metric_alarm(
        AlarmName="ScaleUp"+self.res["AutoScaling"]["MetricName"],
        AlarmDescription="Scale Up for Auto Scaling",
        ActionsEnabled=True,
        AlarmActions=[ScaleUpPolicyARN],
        MetricName=self.res["AutoScaling"]["MetricName"],
        Namespace=self.res["AutoScaling"]["Namespace"],
        Statistic=self.res["AutoScaling"]["Statistic"],
            Dimensions=[
        {
            "Name":"AutoScalingGroupName",
            "Value": self.ASName
        },
                ],
        Period=int(self.res["AutoScaling"]["Period"]),
        Unit=self.res["AutoScaling"]["Unit"],
        EvaluationPeriods=int(self.res["AutoScaling"]["EvaluationPeriods"]),
        Threshold=int(self.res["AutoScaling"]["ThresholdScaleUp"]),
        ComparisonOperator="GreaterThanThreshold"
        )
        #............................................................
        #Alarm ScaleDown
        response = client.put_metric_alarm(
        AlarmName="ScaleDown"+self.res["AutoScaling"]["MetricName"],
        AlarmDescription="Scale Down for Auto Scaling",
        ActionsEnabled=True,
        AlarmActions=[ScaleDownPolicyARN],
        MetricName=self.res["AutoScaling"]["MetricName"],
        Namespace=self.res["AutoScaling"]["Namespace"],
        Statistic=self.res["AutoScaling"]["Statistic"],
            Dimensions=[
        {
            "Name":"AutoScalingGroupName",
            "Value": self.ASName
        },
                ],
        Period=int(self.res["AutoScaling"]["Period"]),
        Unit=self.res["AutoScaling"]["Unit"],
        EvaluationPeriods=int(self.res["AutoScaling"]["EvaluationPeriods"]),
        Threshold=int(self.res["AutoScaling"]["ThresholdScaleDown"]),
        ComparisonOperator="LessThanOrEqualToThreshold"
        )
#===========================================================
#============= Usage example================================
if __name__ == "__main__":
    import VPCUtility
    import LBUtility
    import sys
    import time
#-----------------------------------------------------------
    def initVPC():
        MyVPC=VPCUtility.VPCUtility("ProjectResources.yml")
        print MyVPC.VpcName
        if not MyVPC.Vpc:
            print "Creating a new VPC"
            MyVPC.create_vpc()
        return MyVPC
#-----------------------------------------------------------
    def initLB(VPC):
        MyLB=LBUtility.LBUtility("ProjectResources.yml")
        if MyLB.DNSName:
            print "LB exists..."
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
            MyLB.create_lb([SecurityGroup],SubnetIDs)
            print "Load Balancer is ready - %s" %MyLB
        return MyLB
#-----------------------------------------------------------
    def initAS(LB,VPC):
        MyAS=ASUtility("ProjectResources.yml")
        #Create a SecurityGroup
        if not(MyAS.LCARN) and not(MyAS.ASARN):
            SecurityGroupId=""
            try:
                SecurityGroupId=VPC.GetSecurityGroupId("WebGroup")[0]
            except (ValueError,IndexError):
                pass
            if not(SecurityGroupId):
                SecurityGroupId=VPC.create_security_group("WebGroup")
            print "Creating Launch Config and Auto Scaling..."
            PublicSubnets=",".join(VPC.GetPublicSubnets())
            AvailabilityZones=[]
            for Subnet in VPC.Vpc.subnets.all():
                if Subnet.id  in PublicSubnets:
                    AvailabilityZones.append(Subnet.availability_zone)
            MyAS.create(
            InstanceName="Web",
            SecurityGroup=SecurityGroupId,
            AvailabilityZones=AvailabilityZones,
            LBName=LB.LBName,
            VPCSubnetIds=PublicSubnets,
            TopicARN="arn:aws:sns:us-west-2:12345:Test"
                )   
        else:
            print "Auto Scaling group already exists with ARN:  %s" %MyAS.ASARN
        return MyAS
#-----------------------------------------------------------
    Action = sys.argv[1]
    if Action=="Create":
        VPC=initVPC()
        LB=initLB(VPC)
        AutoScal=initAS(LB,VPC)
    elif Action=="Status":
        VPC=initVPC()
        LB=initLB(VPC)
        print LB.getStatus()
    elif Action=="Delete":
        VPCToDelete=initVPC()
        LBToDelete=initLB(VPCToDelete)
        AutoScalToDelete=initAS(LBToDelete,VPCToDelete)
        AutoScalToDelete.delete_as()
        AutoScalToDelete.delete_lc()
        time.sleep(300)
        LBToDelete.delete_lb()
        VPCToDelete.delete_vpc()
