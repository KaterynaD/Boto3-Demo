#!/usr/bin/python

import boto3
import yaml
import sys
import os.path
"""
VPCtility.py - 03.14.16 Kate Drogaieva
The class in this module allows to create a VPC with subnets, gateway, route, route table
NAT instance, other EC2 instances, security groups
"""
class VPCUtility(object):
    """
    VPCUtility class can be used to create a VPC and its subnets, internet gateway, 
    route table, route, security groups, instances, NAT instance
    The configuration parameters must be provided in a YAML resource file
Region: "us-west-2"
VPC: 
  Name: Test
  CidrBlock: 10.0.0.0/16
  Instance: 
    - 
      AssociatePublicIpAddress: "True"
      DeleteOnTermination: "True"
      DeviceIndex: "0"
      IamInstanceProfileName: ec2-admin
      ImageId: ami-f0091d91
      InstanceInitiatedShutdownBehavior: stop
      InstanceType: t2.micro
      KeyName: aws-csa
      Name: Web
      SecurityGroup: WebGroup
      UserData: WebInstanceUserData.bash
  SecurityGroup: 
    - 
      Description: "To access the instance via SSH and web"
      InboundRules: "SSH,CustomTCPRule8080"
      Name: WebGroup
      OutboundRules: ""
  SecurityGroupRules: 
    - 
      CidrIp: 0.0.0.0/0
      FromPort: "22"
      IpProtocol: TCP
      Name: SSH
      ToPort: "22"
  Subnet: 
    - 
      AvailabilityZone: us-west-2a
      CidrBlock: 10.0.1.0/24
      Name: Private1
      isPublic: "False"

    """
    def __init__(self,resource="",VPCName=""):
        """
            A VPC can be init via a name (VPCName) or a resource YAML file (resource)
            If you plan to create a new VPC you need a resource file with configuraton parameters
            For an already created VPC you can use VPCName only to get VPC components and attributes
        """
        self.Vpc = False
        self.res = False
        if resource:
            try:
                with open(resource, "r") as f:
                    self.res = yaml.load(f)
                self.VpcName = self.res["VPC"]["Name"]
                self.ec2=boto3.resource("ec2",self.res["Region"])
                self.GetVpcId()
            except KeyError or IOError:
                raise ValueError("Wrong VPC parameters")
        elif VPCName:
            self.VpcName = VPCName
            self.ec2=boto3.resource("ec2")
            self.GetVpcId()
        else:
            raise ValueError("Please provide a resource file name or VPC name")
        return
#-----------------------------------------------------------
    def __str__(self):
        return self.VpcName
#-----------------------------------------------------------
    def create_vpc(self):
        """
            The function creates VPC, internet gateway, route table, route and subnets
            All components are taged with names
        """
        if not(self.res):
            raise ValueError("Please provide a resource file to create VPC")
        self.Vpc = self.ec2.create_vpc(CidrBlock=self.res["VPC"]["CidrBlock"],InstanceTenancy="default")
        response = self.Vpc.create_tags(Tags=[{"Key": "Name","Value": self.VpcName}])
        self.create_internet_gateway()
        self.create_route_table()
        self.create_route()
        for Subnet in self.res["VPC"]["Subnet"]:
            SubnetId=self.create_subnet(Subnet)
            if Subnet["isPublic"]=="True":
                self.add_subnet_to_route_table(SubnetId)
        return
#-----------------------------------------------------------
    def create_subnet(self,Subnet):
        """
            The function creates a Subnet according to Subnet parameter which is a dictionary from the resource file
                  AvailabilityZone: us-west-2a
                  CidrBlock: 10.0.1.0/24
                  Name: Private1
                  isPublic: "False"
        """
        self.subnet = self.Vpc.create_subnet(CidrBlock=Subnet["CidrBlock"],AvailabilityZone=Subnet["AvailabilityZone"])
        response = self.subnet.create_tags(Tags=[{"Key": "Name","Value": self.VpcName+"_"+Subnet["Name"]}])
        return self.subnet.id
#-----------------------------------------------------------
    def create_internet_gateway(self):
        """
            The function creates an internet gateway and attach it to the vpc
            The name tag of it is VpcName_IntGtwy
        """
        self.gateway = self.ec2.create_internet_gateway()
        self.gateway.attach_to_vpc(VpcId=self.Vpc.id)
        response = self.gateway.create_tags(Tags=[{"Key": "Name","Value": self.VpcName+"_IntGtwy"}])
        return self.gateway.id
#-----------------------------------------------------------
    def create_route_table(self):
        """
            The function creates a route table in the vpc
            The name tag is VpcName__RtTbl
        """
        self.RouteTable = self.Vpc.create_route_table()
        response = self.RouteTable.create_tags(Tags=[{"Key": "Name","Value": self.VpcName+"_RtTbl"}])
        return self.RouteTable.id
#-----------------------------------------------------------
    def create_route(self):
        """
            The function creates a route for a gateway created earlier
        """
        response = self.RouteTable.create_route(DestinationCidrBlock="0.0.0.0/0",GatewayId=self.gateway.id)
        return response
#-----------------------------------------------------------
    def add_subnet_to_route_table(self,SubnetId):
        """
            The function adds a SubnetId to the route table created earlier
        """
        response = self.RouteTable.associate_with_subnet(SubnetId=SubnetId)
        return response
#-----------------------------------------------------------
    def GetVpcId(self):
        """
            The function return a VPC Id for the class defined by name in init
        """
        try:
            filters = [{"Name":"tag:Name", "Values":[self.VpcName]}]
            self.Vpc = list(self.ec2.vpcs.filter(Filters=filters))[0]
        except IndexError:
            return
        return self.Vpc.id
#-----------------------------------------------------------
    def GetSubnets(self,isPublic=True):
        """
            The function returns a list of Subnets, public if isPublic=True or private othervise
        """
        SubnetIds=[]
        for Subnet in self.Vpc.subnets.all():
            SubnetIds.append(Subnet.id)
        PublicSubnetIds=[]
        for RouteTable in self.Vpc.route_tables.all():
            for Route in RouteTable.routes:
                try:
                    if Route["GatewayId"] != "local":
                        for association in RouteTable.associations.all():
                            PublicSubnetIds.append(association.subnet_id)
                except KeyError:
                    pass
        PrivateSubnetIds=list(set(SubnetIds)-set(PublicSubnetIds))
        if isPublic:
            SubnetIds = PublicSubnetIds
        else:
            SubnetIds =  PrivateSubnetIds
        return SubnetIds
#-----------------------------------------------------------
    def GetPublicSubnets(self):
        """
        It's a short cut to call GetSubnets(isPublic=True) 
        """
        return self.GetSubnets(isPublic=True)
#-----------------------------------------------------------
    def GetPrivateSubnets(self):
        """
        It's a short cut to call GetSubnets(isPublic=False) 
        """
        return self.GetSubnets(isPublic=False)
#-----------------------------------------------------------
    def delete_vpc(self):
        """
            Deletes VPC's: instances (terimnates and waits), security groups, route tables, internet gateway, subnets
        """
        #instances
        for Instance in self.Vpc.instances.all():
            Instance.terminate()
            Instance.wait_until_terminated()
        #Security Groups
        for SecurityGroup in self.Vpc.security_groups.all():
            if SecurityGroup.group_name<>"default":
                SecurityGroup.delete()
        #RouteTables
        MainRouteTable=False
        for RouteTable in self.Vpc.route_tables.all():
            for association in RouteTable.associations.all():
                MainRouteTable=association.main
                if not(association.main):
                    association.delete()
            if not(MainRouteTable):
                RouteTable.delete()
        #internet gateways
        for Internet_Gateway in self.Vpc.internet_gateways.all():
            Internet_Gateway.detach_from_vpc(VpcId=self.Vpc.id)
            Internet_Gateway.delete()
        #subnets
        for Subnet in self.Vpc.subnets.all():
            Subnet.delete()
        self.Vpc.delete()
        #accepted_vpc_peering_connections
        #network_acls
        #network_interfaces
        #requested_vpc_peering_connections
        return
#-----------------------------------------------------------
    def create_security_group(self,GroupName):
        """
            Creates a security group taking the configuration parameters from the resource file 
            by GroupName 
            SecurityGroupRules: 
                - 
                  CidrIp: 0.0.0.0/0
                  FromPort: "22"
                  IpProtocol: TCP
                  Name: SSH
                  ToPort: "22"
        """
        if not(self.res):
            raise ValueError("Please provide a resource file to create a VPC security group")
        for SecurityGroup in self.res["VPC"]["SecurityGroup"]:
            if SecurityGroup["Name"]==GroupName:
                self.SecurityGroup = self.Vpc.create_security_group(GroupName=SecurityGroup["Name"],Description=SecurityGroup["Description"])
                InboundRules=SecurityGroup["InboundRules"].split(",")
                OutboundRules=SecurityGroup["OutboundRules"].split(",")
                #Inbound rules
                for SecurityGroupRule in self.res["VPC"]["SecurityGroupRules"]:
                    for i in xrange(len(InboundRules)):
                        if SecurityGroupRule["Name"]==InboundRules[i]:
                            self.SecurityGroup.authorize_ingress(IpProtocol=SecurityGroupRule["IpProtocol"]
                                            ,CidrIp=SecurityGroupRule["CidrIp"]
                                            ,FromPort=int(SecurityGroupRule["FromPort"])
                                            ,ToPort=int(SecurityGroupRule["ToPort"]))
                #Outbound rules
                for SecurityGroupRule in self.res["VPC"]["SecurityGroupRules"]:
                    for i in xrange(len(OutboundRules)):
                        if SecurityGroupRule["Name"]==OutboundRules[i]:
                            self.SecurityGroup.authorize_egress(IpProtocol=SecurityGroupRule["IpProtocol"]
                                            ,CidrIp=SecurityGroupRule["CidrIp"]
                                            ,FromPort=int(SecurityGroupRule["FromPort"])
                                            ,ToPort=int(SecurityGroupRule["ToPort"]))
        return self.SecurityGroup.id
#-----------------------------------------------------------
    def create_instance(self,InstanceName,SubnetId):
        """
            Creates an instance (and a correspondent security group) 
            in a vpc SubnetId using InstanceName to get the configuration parameters
            from the resource file
              Instance: 
                - 
                  AssociatePublicIpAddress: "True"
                  DeleteOnTermination: "True"
                  DeviceIndex: "0"
                  IamInstanceProfileName: ec2-admin
                  ImageId: ami-f0091d91
                  InstanceInitiatedShutdownBehavior: stop
                  InstanceType: t2.micro
                  KeyName: aws-csa
                  Name: Web
                  SecurityGroup: WebGroup
                  UserData: WebInstanceUserData.bash
            Tags the instance with the name
        """
        if not(self.res):
            raise ValueError("Please provide a resource file to create a VPC instance")
        for Instance in self.res["VPC"]["Instance"]:
            if Instance["Name"]==InstanceName:
                SecurityGroupId=""
                try:
                    SecurityGroupId=self.GetSecurityGroupId(Instance["SecurityGroup"])[0]
                except (ValueError,IndexError):
                    pass
                if not(SecurityGroupId):
                    SecurityGroupId=self.create_security_group(Instance["SecurityGroup"])
                Script=""
                try:
                    if Instance["UserData"]:
                        Script=open(Instance["UserData"], "r").read()
                except KeyError or IOError:
                    print "UserData script can not be open for instance %s" %InstanceName
                AssociatePublicIpAddress=False
                if Instance["AssociatePublicIpAddress"]=="True":
                    AssociatePublicIpAddress=True
                DeleteOnTermination=False
                if Instance["DeleteOnTermination"]=="True":
                    DeleteOnTermination=True
                instances=self.ec2.create_instances(ImageId=Instance["ImageId"]
                            , MinCount=1
                            , MaxCount=1
                            , KeyName=Instance["KeyName"]
                            , UserData=Script
                            , InstanceType=Instance["InstanceType"]
                            , InstanceInitiatedShutdownBehavior=Instance["InstanceInitiatedShutdownBehavior"]
                            , NetworkInterfaces=[
                            {
                            "DeviceIndex":int(Instance["DeviceIndex"])
                            ,"SubnetId": SubnetId
                            ,"DeleteOnTermination": DeleteOnTermination
                            ,"AssociatePublicIpAddress": AssociatePublicIpAddress
                            ,"Groups": [SecurityGroupId]
                            }]
                            ,IamInstanceProfile={
                            "Name": Instance["IamInstanceProfileName"]
                            })
                for i in xrange(len(instances)):
                    response = instances[i].create_tags(Tags=[{"Key": "Name","Value": Instance["Name"]}])
        return instances[0].id
#-----------------------------------------------------------
    def GetSecurityGroupId(self,SecurityGroupName):
        """
            Returns a security group Id for SecurityGroupName
        """
    #Bug: list( VPC.Vpc.security_groups.filter(GroupNames=["SecurityGroupName"])) search in default VPC for whatever reason
        SecurityGroupIDs=[]
        for sg in self.Vpc.security_groups.all():
            if sg.group_name==SecurityGroupName:
                SecurityGroupIDs.append(sg.group_id)
        return SecurityGroupIDs
#-----------------------------------------------------------
    def GetInstance(self,InstanceName):
        """
            Returns an Instance object for InstanceName
        """
        try:
            filters = [{"Name":"tag:Name", "Values":[InstanceName]}]
            Instance = list(self.ec2.instances.filter(Filters=filters))[0]
        except IndexError:
            return
        return Instance
#-----------------------------------------------------------
    def GetInstanceId(self,InstanceName):
        """
            Returns an Instance Id for InstanceName
        """
        Instance=self.GetInstance(InstanceName)
        return Instance.id
#-----------------------------------------------------------            
    def create_nat_instance(self):
        """
            Creates NAT instance (and a correspondent security group) 
            according to "Nat" instance configuration in the resource file
            Modifies sourceDestCheck
            Creates route in the default rout table
            Tags the instans with the name
        """
        if not(self.res):
            raise ValueError("Please provide a resource file to create a VPC instance")
        #Get a public Subnet
        SubnetId=self.GetPublicSubnets()[0]     
        #Create an instance
        for Instance in self.res["VPC"]["Instance"]:
            if Instance["Name"]=="Nat":
                NatInstanceRes=Instance
        if not(NatInstanceRes):
            raise ValueError("There is no Nat instance configuration")
        SecurityGroupId=""
        try:
            SecurityGroupId=self.GetSecurityGroupId(NatInstanceRes["SecurityGroup"])[0]
        except (ValueError,IndexError):
            pass
        if not(SecurityGroupId):
            SecurityGroupId=self.create_security_group(NatInstanceRes["SecurityGroup"])
        AssociatePublicIpAddress=False
        if NatInstanceRes["AssociatePublicIpAddress"]=="True":
            AssociatePublicIpAddress=True
        DeleteOnTermination=False
        if NatInstanceRes["DeleteOnTermination"]=="True":
            DeleteOnTermination=True
        instances=self.ec2.create_instances(ImageId=NatInstanceRes["ImageId"]
                            , MinCount=1
                            , MaxCount=1
                            , KeyName=NatInstanceRes["KeyName"]
                            , InstanceType=NatInstanceRes["InstanceType"]
                            , InstanceInitiatedShutdownBehavior=NatInstanceRes["InstanceInitiatedShutdownBehavior"]
                            , NetworkInterfaces=[
                            {
                            "DeviceIndex":int(NatInstanceRes["DeviceIndex"])
                            ,"SubnetId": SubnetId
                            ,"DeleteOnTermination": DeleteOnTermination
                            ,"AssociatePublicIpAddress": AssociatePublicIpAddress
                            ,"Groups": [SecurityGroupId]
                            }]
                            )
        NatInstance=instances[0]
        NatInstance.create_tags(Tags=[{"Key": "Name","Value": NatInstanceRes["Name"]}])
        #Disable Source/Destination check and wait when it's up and running for route creation
        NatInstance.modify_attribute(Attribute="sourceDestCheck",Value="False")
        NatInstance.wait_until_running()
        #NatId=self.create_instance("Nat",SubnetId)
        #add a new route into default route table 
        for RouteTable in self.Vpc.route_tables.all():
            for association in RouteTable.associations.all():
                if association.main:
                    RouteTable.create_route(DestinationCidrBlock="0.0.0.0/0",InstanceId=NatInstance.id)
        return NatInstance.id
#===========================================================
#============= Usage example================================    
if __name__ == "__main__":
#-----------------------------------------------------------
    def init():
        MyVPC=VPCUtility("ProjectResources.yml")
        print MyVPC.VpcName
        if not MyVPC.Vpc:
            print "Creating a new VPC"
            MyVPC.create_vpc()
        return MyVPC
#-----------------------------------------------------------
    def status(VPC):
        if VPC.Vpc:
            print "VPC exists"
            print "VPC ID =%s" %VPC.Vpc.id
            print "VPC State = %s" %VPC.Vpc.state
            print "VPC Public subnets:"
            print VPC.GetPublicSubnets()
            print "VPC Private subnets:"
            print VPC.GetPrivateSubnets()
        else:
            print "VPC does not exist"
        return VPC
#-----------------------------------------------------------
    def create_web_instance(VPC):
        SubnetId=VPC.GetPublicSubnets()[0]
        print VPC.create_instance("Web",SubnetId)
        return
#-----------------------------------------------------------
    def create_app_instance(VPC):
        SubnetId=VPC.GetPrivateSubnets()[0]
        print VPC.create_instance("App",SubnetId)
        return
#-----------------------------------------------------------
    def create_redshift_secgroup(VPC):
        SecurityGroupId=VPC.create_security_group("RedshiftAll")
        print SecurityGroupId
        return
#-----------------------------------------------------------
    def delete(VPC):
        print "Deleting..."
        VPC.delete_vpc()
        return
#-----------------------------------------------------------
    Action = sys.argv[1]
    if Action=="Create":
        init()
    elif Action=="Status":
        VPC=init()
        status(VPC)
    elif Action=="AppInstance":
        VPC=init()
        create_app_instance(VPC)
    elif Action=="WebInstance":
        VPC=init()
        create_web_instance(VPC)
    elif Action=="Nat":
        VPC=init()
        VPC.create_nat_instance()
    elif Action=="RedshiftAll":
        VPC=init()
        create_redshift_secgroup(VPC)
    elif Action=="Delete":
        VPCToDelete=init()
        delete(VPCToDelete)

