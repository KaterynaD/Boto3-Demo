<H1>Boto3 Demo</H1>

<p>This is a boto3 demo project to create and use of some basic AWS components. 

<p>The application creates an SQS queue, SNS topic, VPC, public and private subnets,  
route, route table, gateway, NAT instance, Redshift cluster, IAM roles, profile and policy,
S3 buckets, Load Balancer,  Auto Scaling and Launch Configuration.

<p>Redshift cluster is created in a private subnet and is not available to the public.

<p>App instance is a created in a private subnet and is used to load data into the datawarehouse. 
The scripts are called when the instance is created using User Data script.

<p>Two Web instances are created as a result of work of a Load Balancer and Auto Scaling. 
Then  one of the instances is terminated.

<p>A report is available via Load Balancer DNS name or directly from the web instances. 
CherryPy is used as a web server

<p>The application generates a set of a Support department  source data  as a data warehouse example  
(analysts, cases, logs and products) and then loads them in S3 buckets
Redshift reads them  using temprary credentials and convert to a star schema.
The conversion to a star schame is based on SQL files and creates a fact table, 
dimensions, Slowly Changing Dimension type 2,flattening a hierarchy. 

<H1>Installation</H1>

<p>It does not require a specific installation.
You must have python and boto3, psycopg2, cherrypy installed to make it works

<H1>Usage</H1>

<p>Just run StartProject.py to create the infrustructure and EndProject.py to destroy it. 
You may need to review and edit all yml configuration files to adjust the settings. 
Currently the most basic and simple configuration is used to create t2.micro instance or 
one node cluster.
