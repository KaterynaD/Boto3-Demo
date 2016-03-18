#!/usr/bin/python
import boto3
from botocore.client import ClientError
import yaml
"""
SQSUtility.py - 03.14.16 Kate Drogaieva
The class in this module allows to create an SQS Queue, send a message,
receive a message, wait for several messages or just one, delete the queue
"""
class SQSUtility(object):
    """
    SQSUtility class creates a Queue and allows to send a message,
	receive a message, wait for several messages or just one, delete the queue
    The configuration parameters must be in a YAML resource file
    Region: "us-west-2"
    Queue:
      Name: "SystemStatus"
    """
    def __str__(self):
        return self.QueueUrl
#-----------------------------------------------------------
    def __init__(self,resource="",QueueName=""):
        """
            A Queue can be created based on QueueName parameter or a resource file in YAML format
        """
        self.QueueUrl=""
        self.res={}
        if QueueName:
            self.QueueName = QueueName
            self.client = boto3.client("sqs")
        elif resource:
            try:
                with open(resource, "r") as f:
                    self.res = yaml.load(f)
                self.QueueName = self.res["Queue"]["Name"]
                self.client = boto3.client("sqs",self.res["Region"])
            except KeyError or IOError:
                sys.exit("Wrong Queue parameters")
        else:
            sys.exit("Please provide QueueName or a valid resource file in yml format with a queue name in [""Queue""][""Name""]")
        self.GetQueueUrl=self.GetQueueUrl()
#-----------------------------------------------------------
    def create_queue(self):
        """
            The function creates a new Queue and returns QueueUrl
        """
        response = self.client.create_queue(QueueName=self.QueueName)
        self.QueueUrl= response["QueueUrl"]
        return self.QueueUrl
#-----------------------------------------------------------
    def GetQueueUrl(self):
        """
            The function returns QueueUrl
        """
        try:
            response = self.client.get_queue_url(QueueName=self.QueueName)
            self.QueueUrl= response["QueueUrl"]
        except ClientError:
            self.QueueUrl=""
        return self.QueueUrl
#-----------------------------------------------------------
    def SendMessage(self,message):
        """
            The function sends message(string) to the queue
        """
        response = self.client.send_message( QueueUrl=self.QueueUrl,MessageBody=message)
        return response["MessageId"]
#-----------------------------------------------------------
    def ReceiveMessages(self, WaitTimeSeconds=20,MaxNumberOfMessages=1,VisibilityTimeout=60):
        """
            The function receives new MaxNumberOfMessages (integer, default 1) messages waiting WaitTimeSeconds (integer, default 20 sec)
            VisibilityTimeout is integer and default 60 sec
            The messages are deleted from the queue
            Returns the list of messages (strings)
        """
        response = self.client.receive_message(QueueUrl=self.QueueUrl
        ,WaitTimeSeconds=WaitTimeSeconds
        ,VisibilityTimeout =VisibilityTimeout
        ,MaxNumberOfMessages=MaxNumberOfMessages)
        Messages=[]
        try:
            for i in xrange(0,len(response["Messages"])):
                ReceiptHandle=response["Messages"][i]["ReceiptHandle"]
                Messages.append(response["Messages"][i]["Body"])
                response = self.client.delete_message(QueueUrl=self.QueueUrl,ReceiptHandle=ReceiptHandle)
        except KeyError:
            pass
        return Messages
    def MessagesWait(self,WaitedMessages,TimeToWaitSec):
        """
            The function waits for  messages in WaitedMessages list TimeToWaitSec (integer)
            It returns {"Received": list of received messages,"NotReceived": list of waited, butnot received messages}
        """
        MessagesReceivedFlag=False
        MessagesReceived=[]
        j=0
        Repeats=int(TimeToWaitSec/20)
        while j<Repeats and not(MessagesReceivedFlag):
            Messages=self.ReceiveMessages(WaitTimeSeconds=20)
            MessagesReceived=MessagesReceived+list(set(WaitedMessages)&set(Messages))
            WaitedMessages=list(set(WaitedMessages)-set(Messages))
            for Message in Messages:
                print "Received: %s" %Message
            if len(WaitedMessages)==0:
                MessagesReceivedFlag=True
            else:
                for WaitedMessage in WaitedMessages:
                    print "Waiting for: %s" %WaitedMessage
            j+=1
        return {"Received": MessagesReceived,"NotReceived":WaitedMessages}
    def MessageWait(self,WaitedMessage,TimeToWaitSec):
        """
            The function waits for a message in WaitedMessage(string) TimeToWaitSec (integer)
            It returns True if the message received or False if it was not received in TimeToWaitSec sec
        """
        ReceivedMessages=self.MessagesWait([WaitedMessage],TimeToWaitSec)
        ReceivedFlag=WaitedMessage in ReceivedMessages["Received"]
        return ReceivedFlag
#-----------------------------------------------------------
    def QueueDelete(self):
        """
            The function deletes the Queue
        """
        response = self.client.delete_queue(QueueUrl=self.QueueUrl)
        return response
#===========================================================
#============= Usage example================================
if __name__ == "__main__":
    import sys
#-----------------------------------------------------------
    def init():
        MyQueue=SQSUtility(QueueName="TestQueue")
        if not MyQueue.QueueUrl:
            print "Creating a new Queue"
            MyQueue.create_queue()
        else:
            print "Queue exists with: %s" %MyQueue
        return MyQueue
#-----------------------------------------------------------
    Action = sys.argv[1]
    if Action=="Create":
        init()
    elif Action=="Send":
        MyQueue=init()
        MyQueue.SendMessage(sys.argv[2])
    elif Action=="Receive":
        MyQueue=init()
        Messages=MyQueue.ReceiveMessages(20)
        for Message in Messages:
            print Message
        if not(Messages):
            print "No new messages"
    elif Action=="Wait":
        MyQueue=init()
        if MyQueue.MessageWait("One",120):
            print "Message received"
        else:
            print "Message was not received"
    elif Action=="WaitMessages":
        MyQueue=init()
        Messages=MyQueue.MessagesWait(["One","Two"],120)
        if not Messages["NotReceived"]:
            print "All messages received"
        else:
            print "Not all messages were received"
            print "Received: "
            for Message in Messages["Received"]:
                print Message
            print "Not received: "
            for Message in Messages["NotReceived"]:
                print Message           
    elif Action=="Delete":
        MyQueue=init()
        print "Deleting..."
        MyQueue.QueueDelete()
