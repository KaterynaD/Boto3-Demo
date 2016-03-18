#!/usr/bin/python
import boto3
from botocore.client import ClientError
import yaml
import sys
"""
SNSUtility.py - 03.14.16 Kate Drogaieva
The class in this module allows to create an SNS Topic, subscribe an email address, publish a message
and delete the topic
"""
class SNSUtility(object):
    """
	SNSUtility class allows to create an SNS Topic, subscribe an email address, publish a message 
	and delete the topic		
    The configuration parameters must be in a YAML resource file
    Region: "us-west-2"
    Topic:
      Name: "SystemStatus"
    """
    def __str__(self):
        return self.TopicArn
#-----------------------------------------------------------
    def __init__(self,resource="",TopicName=""):
        """
            A Topic can be created based on TopicName parameter or a resource file in YAML format
        """

        if TopicName:
            self.TopicName = TopicName
            self.client = boto3.client("sns")
        elif resource:
            try:
                with open(resource, "r") as f:
                    self.res = yaml.load(f)
                self.TopicName = self.res["Topic"]["Name"]
                self.client = boto3.client("sns",self.res["Region"])
            except KeyError or IOError:
                sys.exit("Wrong Topic parameters")
        else:
            sys.exit("Please provide TopicName or a valid resource file in yml format with a topic name in [""Topic""][""Name""]")
        self.TopicArn=self.GetTopicArn()
#-----------------------------------------------------------
    def create_topic(self):
	"""
	The function creates a new topic
	"""
        response = self.client.create_topic(Name=self.TopicName)
        self.TopicArn=response["TopicArn"]
        response = self.client.set_topic_attributes(
        TopicArn=self.TopicArn,
        AttributeName="DisplayName",
        AttributeValue=self.TopicName
        )
        return
#-----------------------------------------------------------
    def GetTopicArn(self):
	"""
	The function returns a topic arn
	"""
        self.TopicArn=""
        response = self.client.list_topics()
        for topic in response["Topics"]:
            if self.client.get_topic_attributes(TopicArn=topic["TopicArn"])["Attributes"]["DisplayName"]==self.TopicName:
                self.TopicArn=topic["TopicArn"]
        return self.TopicArn
#-----------------------------------------------------------
    def TopicSubscribeEmail(self,email):
	"""
	The function subscribes an email (string, valid email address) to the topic
	"""
        response = self.client.subscribe(
        TopicArn=self.TopicArn,
        Protocol="email",
        Endpoint=email
        )
        return response["SubscriptionArn"]
#-----------------------------------------------------------
    def TopicPublish(self,subject,message):
	"""
	The function publishes a new message (string) with subject (string)
			to the topic
	"""
        response = self.client.publish(
        TopicArn=self.TopicArn,
        Message=message,
        Subject=subject
        )
        return response["MessageId"]
#-----------------------------------------------------------
    def TopicDelete(self):
	"""
        The function deletes the topic
	"""
        response = self.client.delete_topic(TopicArn=self.TopicArn)
        return
#==========================================================
#============= Usage example===============================        
if __name__ == "__main__":
    import sys
#-----------------------------------------------------------
    def init():
        MyTopic=SNSUtility(TopicName="TestTopic")
        if not MyTopic.TopicArn:
            print "Creating a new Topic"
            MyTopic.create_topic()
            print "New topic arn: %s" %MyTopic
        else:
            print "Topic exists with: %s" %MyTopic
        return MyTopic
#-----------------------------------------------------------
    Action = sys.argv[1]
    if Action=="Create":
        init()
    elif Action=="Subscribe":
        MyTopic=init()
    	response=MyTopic.TopicSubscribeEmail(sys.argv[2])
        print "New Subscription ID: %s" %response
    elif Action=="Publish":
        MyTopic=init()
	response=MyTopic.TopicPublish("Hello","This is a message from an AWS topic")
        print "New Message ID: s%" %response
    elif Action=="Delete":
        MyTopic=init()
        print "Deleteting"
        MyTopic.TopicDelete()
