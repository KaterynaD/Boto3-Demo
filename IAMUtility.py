#!/usr/bin/python
import boto3
from botocore.client import ClientError
import yaml
import json
"""
IAMUtility.py - 03.14.16 Kate Drogaieva
The classes in this module allows to create an IAM Role, Policy and Instance profile
"""
class RoleUtility(object):
    """
        RoleUtility class creates a role and attaches policies, de-attaches policies and delete the role
    """
    def __init__(self,RoleName=""):
        """
            Initiate the class with RoleName (string)
        """
        self.RoleName=RoleName
        self.client = boto3.client("iam")
        self.RoleArn=self.get_rolearn()



    def get_rolearn(self):
        """
        Returns Role Arn if a role exists.
        """
        try:
            response=self.client.get_role(RoleName=self.RoleName)
            self.RoleArn=response["Role"]["Arn"]
        except ClientError:
            self.RoleArn=""
        return self.RoleArn
#------------------------------------------------------------------------------------------
    def create_role(self,AssumeRolePolicy):
        """
            Creates a role and assignes AssumeRolePolicy (json formatted string)
        """
        response=self.client.create_role(
        RoleName=self.RoleName,
        AssumeRolePolicyDocument=AssumeRolePolicy
        )
        self.RoleArn==response["Role"]["Arn"]
        return self.RoleArn
#------------------------------------------------------------------------------------------
    def add_policy(self,PolicyArn="",PolicyName="",PolicyDocument=""):
        """
            Attaches PolicyArn (string) to the role
            or puts inline policy - PolicyName (string) and PolicyDocument (json formatted string)
        """
        if PolicyArn:
            self.client.attach_role_policy(
            RoleName=self.RoleName,
            PolicyArn=PolicyArn
        )
        elif PolicyName and PolicyDocument:
            self.client.put_role_policy(
            RoleName=self.RoleName,
            PolicyName=PolicyName,
            PolicyDocument=PolicyDocument
        )

#------------------------------------------------------------------------------------------
    def delete_role(self):
        """
            Detaches all policies and deletes the role
        """
        #deattch policies
        response = self.client.list_attached_role_policies(RoleName=self.RoleName)
        for Policy in response["AttachedPolicies"]:
            self.client.detach_role_policy(
            RoleName=self.RoleName,
            PolicyArn=Policy["PolicyArn"]
            )
        #detach profiles
        response = self.client.list_instance_profiles_for_role(RoleName=self.RoleName)
        for Profile in response["InstanceProfiles"]:
            response = self.client.remove_role_from_instance_profile(
            InstanceProfileName=Profile["InstanceProfileName"],
                RoleName=self.RoleName
            )
        #delete the role
        self.client.delete_role(RoleName=self.RoleName)
#==========================================================================================
class PolicyUtility(object):
    """
        PolicyUtility class creates a policy and  deletes it with all its versions
    """
    def __init__(self,PolicyName=""):
        self.PolicyName=PolicyName
        self.client = boto3.client("iam")
        self.PolicyArn=self.get_policyarn()
#------------------------------------------------------------------------------------------
    def get_policyarn(self):
        """
        Returns PolicyArn
        """
        try:
            AccountId= str(boto3.resource("iam").CurrentUser().arn.split(":")[4])
            response=self.client.get_policy(PolicyArn="arn:aws:iam::%s:policy/%s" %(AccountId,self.PolicyName))
            self.PolicyArn=response["Policy"]["Arn"]
        except ClientError:
            self.PolicyArn=""
        return self.PolicyArn
#------------------------------------------------------------------------------------------
    def create_policy(self,PolicyDocument,Description=""):
        """
            Creates policy. PolicyDocument (json formatted string) is a required parameter, Description is a string, optional
            Returns PolicyArn
        """
        response = self.client.create_policy(
    PolicyName=self.PolicyName,
    PolicyDocument=PolicyDocument,
    Description=Description
        )
        self.PolicyArn=response["Policy"]["Arn"]
        return self.PolicyArn
#------------------------------------------------------------------------------------------
    def delete_policy(self):
        """
            Deletes version and then the policy
        """
        response=self.client.list_policy_versions(PolicyArn=self.PolicyArn)
        for Version in response["Versions"]:
            if not(Version["IsDefaultVersion"]):
                self.client.client.delete_policy_version(
                    PolicyArn=self.PolicyArn,
                    VersionId=Version["Version"]
                    )
        self.client.delete_policy(PolicyArn=self.PolicyArn)
#===========================================================
class InstanceProfileUtility(object):
    """
        InstanceProfileUtility class creates an instance profile, adds role and delete the profile
    """
    def __init__(self,ProfileName=""):
        self.ProfileName=ProfileName
        self.client = boto3.client("iam")
        self.ProfileArn=self.get_profilearn()
#------------------------------------------------------------------------------------------
    def get_profilearn(self):
        """
            Returns Instance Profile Arn
        """
        try:
            response = self.client.get_instance_profile(InstanceProfileName=self.ProfileName)
            self.ProfileArn=response["InstanceProfile"]["Arn"]
        except ClientError:
            self.ProfileArn=""
        return self.ProfileArn
#------------------------------------------------------------------------------------------
    def create_profile(self):
        """
            Creates Instance profile
        """
        response = self.client.create_instance_profile(InstanceProfileName=self.ProfileName)
        self.ProfileArn=response["InstanceProfile"]["Arn"]
        return self.ProfileArn
#------------------------------------------------------------------------------------------
    def add_role(self, RoleName):
        """
            Adds a role (ROleName) to the instance profile
        """
        response = self.client.add_role_to_instance_profile(
        InstanceProfileName=self.ProfileName,
        RoleName=RoleName
        )
#------------------------------------------------------------------------------------------
    def delete_profile(self):
        """
            Deletes the instance profile
        """
        response = self.client.delete_instance_profile(
        InstanceProfileName=self.ProfileName
        )
#------------------------------------------------------------------------------------------
#===========================================================
#============= Usage example================================
if __name__ == "__main__":
    import sys
#-----------------------------------------------------------
    try:
        ResourceFile=sys.argv[1]
    except:
        ResourceFile="ProjectResources.yml"
    print "Creates roles and policies and grants read-only access to S3 buckets from %s." %ResourceFile
    with open(ResourceFile, "r") as f:
        res = yaml.load(f)
    #Current Account Id
    AccountId= str(boto3.resource("iam").CurrentUser().arn.split(":")[4])
    print "Creating TempBucketsAccessRole..."
    #Roles and policies for temprary credentials to load data from an S3 bucket to Redshift
    tempaccess_basic_role_policy=json.loads(res["IAM"]["TempBucketAccessRole"]["Basic_Policy"].replace("'","\""))
    tempaccess_role=RoleUtility(res["IAM"]["TempBucketAccessRole"]["Name"])
    if not(tempaccess_role.RoleArn):
        tempaccess_basic_role_policy["Statement"][0]["Principal"]["AWS"]="arn:aws:iam::%s:root"%AccountId
        tempaccess_role.create_role(json.dumps(tempaccess_basic_role_policy))
        PoliciesArns=res["IAM"]["TempBucketAccessRole"]["Other_Policies"].split(",")
        for PolicyArn in PoliciesArns:
            tempaccess_role.add_policy(PolicyArn=PolicyArn)
    else:
        print "TempBucketsAccessRole already exists: %s" %tempaccess_role.RoleArn
    #Role and policies for Web and App instances
    print "Creating InstanceAdminRole..."
    instance_role=RoleUtility(res["IAM"]["InstanceRole"]["Name"])
    if not(instance_role.RoleArn):
        instance_role.create_role(res["IAM"]["InstanceRole"]["Basic_Policy"].replace("'","\""))
        PoliciesArns=res["IAM"]["InstanceRole"]["Other_Policies"].split(",")
        for PolicyArn in PoliciesArns:
            instance_role.add_policy(PolicyArn=PolicyArn)
        #Assume policy to provide temp credentials for AWS
        print "Creating Assume Policy..."
        assume_policy=PolicyUtility(res["IAM"]["AccessPolicy"]["Name"])
        if not(assume_policy.PolicyArn):
            assume_policy.create_policy(res["IAM"]["AccessPolicy"]["Policy_doc"].replace("'","\""),res["IAM"]["AccessPolicy"]["Description"])
        else:
            print "Assume Policy already exists: %s"  %assume_policy.PolicyArn
        instance_role.add_policy(PolicyArn=assume_policy.PolicyArn)
    else:
        print "InstanceAdminRole already exists: %s" %instance_role.RoleArn
    #InstanceProfile
    instance_profile=InstanceProfileUtility(res["IAM"]["InstanceRole"]["Name"])
    if not(instance_profile.ProfileArn):
        instance_profile.create_profile()
        instance_profile.add_role(res["IAM"]["InstanceRole"]["Name"])
    else:
        print "Instance profile already exists: %s" %instance_profile.ProfileArn
    
