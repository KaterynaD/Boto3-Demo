#!/usr/bin/python
import SQSUtility
import sys
"""
SendToQueue.py - 03.14.16 Kate Drogaieva
The module allows to send a message to a queue from a command line
1st argument is an yml formatted file with a queue name. The program expects:
Queue:
  Name: "SystemStatus"
2nd argument is the message itself
"""
try:
    ResourceFile=sys.argv[1]
except:
    ResourceFile="ProjectResources.yml"
MyQueue=SQSUtility.SQSUtility(resource=ResourceFile)
if not MyQueue.QueueUrl:
    print "Creating a new Queue"
    MyQueue.create_queue()
    print "New Queue url %s" %MyQueue
else:
    print "Queue exists with: %s" %MyQueue
print "Sending message to %s" %MyQueue
print sys.argv[1]
print MyQueue.SendMessage(sys.argv[2])
