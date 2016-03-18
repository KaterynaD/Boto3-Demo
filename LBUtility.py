#!/usr/bin/python
import boto3
from botocore.client import ClientError
import yaml
import sys
import os.path
"""
LBUtility.py - 03.14.16 Kate Drogaieva
The class in this module allows to create a Load Balancer
"""
class LBUtility(object):
    """
    LBUtility class can be used to create a Load Balancer according to the configuration parameters
    in YAML resource file:
    Region: "us-west-2"
    LB:
      Name: "TestLB"
      Protocol: "HTTP"
      LoadBalancerPort: "8080"
      InstanceProtocol: "HTTP"
      InstancePort: "8080"
      Target: "HTTP:8080/"
      Interval: "10"
      Timeout: "5"
      UnhealthyThreshold: "2"
      HealthyThreshold: "3"
    """
    def __init__(self,resource="",LBName=""):
        """
            A Load Balancer can be init via a name (LBName) or a resource YAML file (resource)
            If you plan to create a new load balancer you need a resource file with configuraton parameters
            For an already created load balancer you can use LBName only to get load balancer  attributes
        """
        self.res = False
        if resource:
            try:
                with open(resource, "r") as f:
                    self.res = yaml.load(f)
                self.LBName = self.res["LB"]["Name"]
                self.client = boto3.client("elb",self.res["Region"])
                try:
                    self.DNSName=self.getDNSName()
                except ClientError:
                    self.DNSName=""
            except KeyError or IOError:
                sys.exit("Wrong LB parameters")
        elif LBName:
            self.LBName = LBName
            self.client = boto3.client("elb")
            try:
                self.DNSName=self.getDNSName()
            except ClientError:
                self.DNSName=""
        else:
            raise ValueError("Please provide a resource file name or LB name")
        return
#-----------------------------------------------------------
    def __str__(self):
        return self.DNSName
#-----------------------------------------------------------
    def create_lb(self,SecurityGroup,Subnets=[]):
        """
            Creates a load balancer in provided SecurityGroup(string) and Subnets(list)
            Then the function configures the load balancer health check
            The configuration parameters must be in YAML resource file
        """
        if not(self.res):
            raise ValueError("Please provide a resource file to create LB")
        response = self.client.create_load_balancer(
        LoadBalancerName=self.LBName,
        Listeners=[
        {
            "Protocol": self.res["LB"]["Protocol"],
            "LoadBalancerPort": int(self.res["LB"]["LoadBalancerPort"]),
            "InstanceProtocol": self.res["LB"]["InstanceProtocol"],
            "InstancePort":  int(self.res["LB"]["InstancePort"])
        },
                ],
        Subnets=Subnets,
        SecurityGroups=SecurityGroup
        )
        self.DNSName=response["DNSName"]
        #Health check
        response = self.client.configure_health_check(
        LoadBalancerName=self.LBName,
        HealthCheck={
        "Target": self.res["LB"]["Target"],
        "Interval": int(self.res["LB"]["Interval"]),
        "Timeout": int(self.res["LB"]["Timeout"]),
        "UnhealthyThreshold": int(self.res["LB"]["UnhealthyThreshold"]),
        "HealthyThreshold": int(self.res["LB"]["HealthyThreshold"])
        }
        )
        return self.DNSName
#-----------------------------------------------------------
    def register_instance(self, InstanceId):
        """
            The function registes InstanceId in the load balancer
        """
        response = self.client.register_instances_with_load_balancer(
        LoadBalancerName=self.LBName,
        Instances=[
        {
            "InstanceId": InstanceId
        }
              ]
                )
#-----------------------------------------------------------
    def delete_lb(self):
        """
            Deletes the load balancer
        """
        response = self.client.delete_load_balancer(
            LoadBalancerName=self.LBName
        )
#-----------------------------------------------------------
    def getDNSName(self):
        """
            The function returns DNSName of the load balancer
        """
        response = self.client.describe_load_balancers(
        LoadBalancerNames=[self.LBName])
        try:
            self.DNSName=response["LoadBalancerDescriptions"][0]["DNSName"]
        except KeyError:
            self.DNSName=""
        return self.DNSName
#-----------------------------------------------------------
    def getListener(self):
        """
            The function returns Listener attributes of the load balancer as a dictionary
        """
        response = self.client.describe_load_balancers(
        LoadBalancerNames=[self.LBName])
        try:
            Listener=response["LoadBalancerDescriptions"][0]["ListenerDescriptions"][0]["Listener"]
        except KeyError:
            Listener={}
        return Listener 
#-----------------------------------------------------------
    def getInstancesHealth(self):
        """
            The function returns instance health check in JSON format
        """
        response = self.client.describe_instance_health(LoadBalancerName=self.LBName)
        return response
#-----------------------------------------------------------
    def getStatus(self):
        """
            The function returns "InService" if at least one instance is "InService" otherwise it returns "OutofService"
        """
        response=self.getInstancesHealth()
        self.Status="OutofService"
        try:
            InstanceStates=response['InstanceStates']
            for InstanceState in InstanceStates:
                if InstanceState['State']=='InService':
                    self.Status="InService"
                    break
        except:
            self.Status="OutofService"
        return self.Status
#===========================================================
#============= Usage example================================        
if __name__ == "__main__":
    import VPCUtility
    import sys

#-----------------------------------------------------------
    def initVPC():
        MyVPC=VPCUtility.VPCUtility("ProjectResources.yml")
        print MyVPC.VpcName
        if not MyVPC.Vpc:
            print "Creating a new VPC"
            MyVPC.create_vpc()
        try:
            WebInstanceId=MyVPC.GetInstanceId("Web")
        except AttributeError: 
            WebInstanceId=""
        if not(WebInstanceId):
            SubnetId=MyVPC.GetPublicSubnets()[0]
            print MyVPC.create_instance("Web",SubnetId)
        return MyVPC
#-----------------------------------------------------------
    def initLB(VPC):
        MyLB=LBUtility("ProjectResources.yml")
        if MyLB.DNSName:
            print "LB exists..."
        else:
            print "Creating new LB..."
            SubnetIDs=VPC.GetPublicSubnets()
            SecurityGroup=VPC.GetSecurityGroupId("WebGroup")
            if not(SubnetIDs):
                sys.exit("Cannot create a new LB because no Subnets")
            if not(SecurityGroup):
                SecurityGroup=VPC.create_security_group("WebGroup")
            MyLB.create_lb(SecurityGroup,SubnetIDs)
            WebInstanceId=VPC.GetInstanceId("Web")
            if WebInstanceId:
                MyLB.register_instance(WebInstanceId)
            else:
                print "Can not find a WebInstance Id to register"
        return MyLB
    Action = sys.argv[1]
    if Action=="Create":
        VPC=initVPC()
        LB=initLB(VPC)
        print str(LB)
    elif Action=="Health":
        VPC=initVPC()
        LB=initLB(VPC)
        print LB.getInstancesHealth()
    elif Action=="Listener":
        VPC=initVPC()
        LB=initLB(VPC)
        print LB.getListener()
    elif Action=="Delete":
        VPC=initVPC()
        LBToDelete=initLB(VPC)
        print "Deleting..."        
        LBToDelete.delete_lb()
