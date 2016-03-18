#!/usr/bin/python
import boto3
from botocore.client import ClientError
import yaml
"""
RedshiftUtility.py - 03.14.16 Kate Drogaieva
The class in this module allows to create a Redshift claster standalone or in VPC
"""
#---------------------------------------------------------------------------------------------
class RedshiftUtility(object):
    """
    RedshiftUtility class can be used to create a Redshift cluster standalone or in VPC
    The configuration parameters must be provided in a YAML resource file
    RedshiftCluster:
      Endpoint:
      DBName: "supportdw"
      ClusterIdentifier: "Support"
      ClusterType: "single-node"
      NodeType: "dc1.large"
      MasterUsername: "masteruser"
      MasterUserPassword: "Kaktus2015"
      Port: "5439"
      PubliclyAccessible: "False"
      Encrypted: "False"
      
      Optional section to create a cluster in a VPC
      
      SubnetGroup:
        Name: "testvpcprivate"
        Description: "Private Subnets in Test VPC"
        VPC: "Test"
        SecurityGroup: "RedshiftAll"
    """
    def __str__(self):
        return self.endpoint+":"+str(self.port)
#---------------------------------------------------------------------------------------------
    def __init__(self,resource):
        """
            Init Redshift cluster parameters using resource file in YAML format
        """
        try:
            with open(resource, "r") as f:
                self.res = yaml.load(f)
            DBName=self.res["RedshiftCluster"]["DBName"],
            self.ClusterIdentifier=self.res["RedshiftCluster"]["ClusterIdentifier"]
        except KeyError or IOError:
            sys.exit("Wrong Cluster Parameters")
        self.client=boto3.client("redshift",self.res["Region"])
        self.endpoint=""
        self.port=""
        self.GetEndpoint()
        return
#---------------------------------------------------------------------------------------------
    def CreateClusterSubnetGroup(self,SubnetIds):
        """
            Creates a cluster Subnet Group if ["RedshiftCluster"]["SubnetGroup"]  exists
            in YAML configuration file
            SubnetIds is a list of public or private subnets from a VPC
            return Subgroup name or default if VPC does not exists
        """
        SubnetGroup="default"
        response = self.client.create_cluster_subnet_group(
                   ClusterSubnetGroupName=self.res["RedshiftCluster"]["SubnetGroup"]["Name"],
                   Description=self.res["RedshiftCluster"]["SubnetGroup"]["Description"],
                   SubnetIds=SubnetIds)
        SubnetGroup=response["ClusterSubnetGroup"]["ClusterSubnetGroupName"]
        return SubnetGroup
#---------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------
    def CreateCluster(self,SubnetIds=[],SecurityGroupId=[]):
        """
        Create a new cluster acoording to the configuration parameters in YAML resource file
        If there is ["RedshiftCluster"]["SubnetGroup"] in the resource file 
        a cluster Subnet Group is created first based on SubnetIds (list) from a VPC
        SecurityGroupId (list) can be used to create the cluster in VPC
        """
        SubnetGroup="default"
        try:
            if self.res["RedshiftCluster"]["SubnetGroup"]:
                try: #test if a group already exists
                    response = self.client.describe_cluster_subnet_groups(ClusterSubnetGroupName=self.res["RedshiftCluster"]["SubnetGroup"]["Name"])
                    SubnetGroup=self.res["RedshiftCluster"]["SubnetGroup"]["Name"]
                except ClientError: #group does not exist - create
                    SubnetGroup=self.CreateClusterSubnetGroup(SubnetIds)
        except KeyError:
            pass
        PubliclyAccessible=True
        if self.res["RedshiftCluster"]["PubliclyAccessible"]=="False":
            PubliclyAccessible=False
        Encrypted=True
        if self.res["RedshiftCluster"]["Encrypted"]=="False":
            Encrypted=False
        response = self.client.create_cluster(
                DBName=self.res["RedshiftCluster"]["DBName"],
                ClusterIdentifier=self.ClusterIdentifier,
                ClusterType=self.res["RedshiftCluster"]["ClusterType"],
                NodeType=self.res["RedshiftCluster"]["NodeType"],
                MasterUsername=self.res["RedshiftCluster"]["MasterUsername"],
                MasterUserPassword=self.res["RedshiftCluster"]["MasterUserPassword"],
                Port=int(self.res["RedshiftCluster"]["Port"]),
                PubliclyAccessible=PubliclyAccessible,
                Encrypted=Encrypted,
                ClusterSubnetGroupName=SubnetGroup,
                VpcSecurityGroupIds=SecurityGroupId
                )
        return
#---------------------------------------------------------------------------------------------
    def DeleteCluster(self):
        """
            The function deletes the cluster and do not create a final cluster snapshot
        """
        response = self.client.delete_cluster(
            ClusterIdentifier=self.ClusterIdentifier,
            SkipFinalClusterSnapshot=True
            )
        return
#---------------------------------------------------------------------------------------------
    def DeleteClusterSubgroup(self):
        """
            The function deletes the cluster Subnet Group if it exists
        """
        try: #to delete a group if  exists
                response = self.client.delete_cluster_subnet_group(ClusterSubnetGroupName=self.res["RedshiftCluster"]["SubnetGroup"]["Name"])
        except (ClientError,KeyError) as e: #group does not exist or not configured in the resource file - pass
            pass
        return
#---------------------------------------------------------------------------------------------
    def GetEndpoint(self):
        """
            The function set endpoint and port variable and returns 0 if no errors
            or 1 if the cluster is not available
        """
        try:
            response = self.client.describe_clusters(
            ClusterIdentifier=self.ClusterIdentifier
            )
            try:
                self.endpoint=response["Clusters"][0]["Endpoint"]["Address"]
                self.port=response["Clusters"][0]["Endpoint"]["Port"]
                return 0
            except KeyError:
                return 1
        except ClientError: #Cluster does not exists
            return 1
#---------------------------------------------------------------------------------------------
    def CheckStatus(self):
        """
            The same as GetEndpoint
        """
        return self.GetEndpoint()
#---------------------------------------------------------------------------------------------
    def WaitForCreation(self):
        """
            The function waits while the cluster is creating It can take significant amount of time
        """
        waiter = self.client.get_waiter("cluster_available")
        try:
            waiter.wait(ClusterIdentifier=self.ClusterIdentifier)
            self.GetEndpoint
            #Cluster is available
            result=0
        except ClientError: #Cluster does not exists
            result=1
        return result
#---------------------------------------------------------------------------------------------
    def WaitForDeletion(self):
        """
            The function waits while the cluster is deleting It can take significant amount of time
        """
        waiter = self.client.get_waiter("cluster_deleted")
        try:
            waiter.wait(ClusterIdentifier=self.ClusterIdentifier)
            #Cluster is available
            result=0
        except ClientError: #Cluster does not exists
            result=1
        return result
#---------------------------------------------------------------------------------------------
#=============================================================================================
#============= Usage example==================================================================
if __name__ == "__main__":
    import sys
    import VPCUtility
    def initVPC():
        MyVPC=VPCUtility.VPCUtility("ProjectResources.yml")
        print MyVPC.VpcName
        if not MyVPC.Vpc:
            print "Creating a new VPC..."
            MyVPC.create_vpc()
        else:
            print "VPC exists"
        return MyVPC
    def init(VPC=None,isPublic=True):
        c=RedshiftUtility("ProjectResources.yml")
        if not(c.endpoint):
            print "Creating cluster. It will take several minutes..."
            if not(VPC):
            #Create standalone cluster
                c.CreateCluster()
            else:
            #Create Cluster in VPC
                if isPublic:
                    SubnetId=MyVPC.GetPublicSubnets()[0]
                else:
                    SubnetId=MyVPC.GetPrivateSubnets()[0]
                SecurityGroupId=MyVPC.GetSecurityGroupId("RedshiftAll")
                c.CreateCluster([SubnetId],SecurityGroupId)
            c.WaitForCreation()
            c.CheckStatus()
            if c.CheckStatus()==0:
                print "Cluster is available: %s" %c
            else:
                print "Cluster is not available..."
        else:
            print "Cluster is available: %s" %c
        return c

        
    Action = sys.argv[1]
    
    if Action=="Create":
        c=init()
    elif Action=="CreateVPC":
        MyVPC=initVPC()
    elif Action=="CreateInVPCPublic":
        MyVPC=initVPC()
        c=init(VPC=MyVPC,isPublic=True)
    elif Action=="CreateInVPCPrivate":
        MyVPC=initVPC()
        c=init(VPC=MyVPC,isPublic=False)
    elif Action=="RedshiftAll":
        MyVPC=initVPC()
        print "Creating Redshift SecurityGroup..."
	RedshiftSecGrp=MyVPC.create_security_group("RedshiftAll")
        print "Scurity group created with ID= %s" %RedshiftSecGrp
    elif Action=="DeleteVPC":
        MyVPC=initVPC()
        print "Deleting VPC..."
        MyVPC.delete_vpc()
    elif Action=="Delete":
        c=init()
        print "Deleting cluster. It will take several minutes..."
        c.DeleteCluster()
        c.WaitForDeletion()
        c.DeleteClusterSubgroup()
    elif Action=="Status":
        c=init()
        if c.CheckStatus()==0:
            print c
        else:
            print "Cluster is not available..."


