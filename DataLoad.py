#!/usr/bin/python
import psycopg2
import os.path
import yaml
"""
DataLoad.py - 03.14.16 Kate Drogaieva
This module provides a class to perform a database table related actions: "create","delete","load","drop" from SQLs in files
"""
class DataLoad(object):
    """
	Performs DB Tables related actions: "create","delete","load","drop" from SQLs in files
	The tables, SQL file names and placeholders are described in JSON format
	{
	"Tables":
	[ {"name" : "d_analysts",
		"description" : "SCD type 2",
		"scripts" : {"create" : "sql/create_d_analysts.sql",
					"load" : "sql/build_scdt2_d_analysts.sql",
					"drop" : "sql/drop_table.sql"}},
		{"name" : "f_cases",
		"description" : "cases fact table",
		"scripts" : {"create" : "sql/create_f_cases.sql",
					"load" : "sql/insert_f_cases.sql",
					"drop" : "sql/drop_table.sql"}}
	]
	"Placeholders" :
	[
	{"name": "#bucket#",
	"value":"kdsupportdata"}
	]
	}
	Example an SQL with a parameter

	copy #table_name# from "s3://#bucket#/#table_name#"
	credentials "aws_access_key_id=#aws_access_key_id#;aws_secret_access_key=#aws_secret_access_key#"
	delimiter "," region "us-west-2" REMOVEQUOTES;
	
	By default #table_name# is replaced with a table name from Tables section and there is no need to describe it in Placeholders section
    """
#---------------------------------------------------------------------------------------------
    def __str__(self):
        s=chr(13)+chr(10)
        s=s+"Tables"
        s=s+chr(13)+chr(10)
        for i in range(self.tables_num):
            s=s+self.tables["Tables"][i]["name"]
            if len(self.tables["Tables"][i]["description"])>0:
                s=s+" - "+self.tables["Tables"][i]["description"]
            s=s+chr(13)+chr(10)
        s=s+chr(13)+chr(10)
        s=s+"Placeholders"
        s=s+chr(13)+chr(10)
        for i in range(self.par_num):
            s=s+self.tables["Placeholders"][i]["name"]
            if len(self.tables["Placeholders"][i]["value"])>0:
                s=s+" - "+self.tables["Placeholders"][i]["value"]
            s=s+chr(13)+chr(10)
        return "%s" % (s)
    #flatten hierarchical dict-list structure
#---------------------------------------------------------------------------------------------
    def getpar (self,d,prev_key="",fd={}):
        """
		d is a resource file content where each memeber can be a dictionary, list or string
		The function flattens it to a dictionary (fd) with "level1.level2.level3":"value" structure
		if an element is a list then the key will look like "level1.listlevel[index].level3"
		This is a recursive function and for inner levels it takes a previous key name and a previous flatten dictionary content
        """
        for key in d:
            if type(d[key]) is str:
                fd[prev_key+key]=d[key]
            elif type(d[key]) is dict:
                self.getpar(d[key],prev_key+key+".",fd)
            elif type(d[key]) is list:
                for i in xrange(len(d[key])):
                    self.getpar(d[key][i],prev_key+key+"["+str(i)+"]"+".",fd)
        return fd
#---------------------------------------------------------------------------------------------
    def __init__(self,tables,connection,resources,bucket_access):
        self.tables=tables
        self.connection=connection
        self.bucket_access=bucket_access
        try:
            self.tables_num=len(self.tables["Tables"])
        except KeyError:
            self.tables_num=0
        try:
            self.par_num=len(self.tables["Placeholders"])
        except KeyError:
            self.par_num=0
        try:
            with open(resources, "r") as f:
                self.res = yaml.load(f)
            #resource file flattening
            self.res_params=self.getpar(self.res)
        except KeyError or IOError:
            self.res_params={}

#---------------------------------------------------------------------------------------------
    def replace_Placeholders(self,sql,table_name):
	"""
	    Replaces Placeholders in SQL text
	"""
        #all Placeholders are in ##
        #default #table_name# is replaced with self.tables["Tables"][i]["name"]
        #optional resource file is used to replace #key# from resource file placeholders
        sql=sql.replace("#table_name#",table_name)
        #If provided - AccessKeyId, SecretAccessKey, Session Token
        sql=sql.replace("#aws_access_key_id#",self.bucket_access["AccessKeyId"])
        sql=sql.replace("#aws_secret_access_key#",self.bucket_access["SecretAccessKey"])
        sql=sql.replace("#aws_session_token#",self.bucket_access["SessionToken"])
        for i in range(self.par_num):
            old = self.tables["Placeholders"][i]["name"]
            new = self.tables["Placeholders"][i]["value"]
            sql=sql.replace(old,new)
        for key in self.res_params:
            old = "#"+key+"#"
            new = self.res_params[key]
            sql=sql.replace(old,new)
        return sql
#---------------------------------------------------------------------------------------------
    def run(self,action):
	"""
	Runs SQL (any) for each table
	"""
        if action not in ["create","delete","load","drop"]:
            print "Can not perform "+action
            return 1
        try:
            cur = self.connection.cursor()
            for i in range(self.tables_num):
                script=self.tables["Tables"][i]["scripts"][action]["file"]
                print script
                try:
                    if self.tables["Tables"][i]["scripts"][action]["StopOnError"]=="False":
                        StopOnError=False
                    else:
                        StopOnError=True
                except KeyError:
                    StopOnError=True #default value
                if os.path.exists(script):
                    sql=self.replace_Placeholders(open(script, "r").read(),self.tables["Tables"][i]["name"])
                    print "Action: " + action +" for "+ self.tables["Tables"][i]["name"] + " from " + script
                    print "................................"
                    print sql
                    print "................................"
                    error_f=0
                    try:
                        cur.execute(sql)
                        self.connection.commit()
                    except Exception as e:
                        print "Error performing " + action + " for " + self.tables["Tables"][i]["name"] + " using " + script
                        print str(e)
                        error_f=1
                        self.connection.rollback()
                        if StopOnError:
                            print "Stop on error"
                            break
                        else:
                            print "Continue despite the error"
                else:
                    print "Cannot perform " + action + " for " + self.tables["Tables"][i]["name"] + " using " + script
                    error_f=1
        except Exception as e:
            error_f=1
        finally:
            cur.close()
        return error_f
#---------------------------------------------------------------------------------------------
