#!/usr/bin/python
import cherrypy
import psycopg2
import yaml
import RedshiftUtility
import boto3
import HTML
"""
WebReport.py - 03.14.16 Kate Drogaieva
This module connects to Redshift, selects data and returns it as an html-report
"""
htmlheader="<html><head><title>Average Performance By Team</title></head><body><H1>Average Performance By Team</H1>"
htmlfooter="</body></html>"
#....................................................................................................................   
def GetData():
    ResourceFile="WebProjectResources.yml"  
    with open(ResourceFile, "r") as f:
        res = yaml.load(f)
#....................................................................................................................
    c=RedshiftUtility.RedshiftUtility(ResourceFile)
    host=c.endpoint
    if not(host):
        return "Redshift Cluster is not available. Stop the program"
    try:
        conn = psycopg2.connect(
        host=host,
        user=res["RedshiftCluster"]["MasterUsername"],
        port=res["RedshiftCluster"]["Port"],
        password=res["RedshiftCluster"]["MasterUserPassword"],
        dbname=res["RedshiftCluster"]["DBName"])
        #query
        try:
            cur = conn.cursor()
            cur.execute(open(res["SQLsFolder"]+"TeamsPerformance.sql", "r").read())
            table_data=[]
            for record in cur:
                table_data.append(record)
            conn.commit()
            htmlcode = HTML.table(table_data,header_row=['Team/SLA',   'Priority 1',   'Priority 2', 'Priority 3'])
            return htmlheader+htmlcode+htmlfooter
        except Exception as e:
            return str(e)
            conn.rollback()
        finally:
            cur.close()
    except KeyError:
        return "Wrong Redshift connect configuration parameters"
    except psycopg2.Error as e:
        return "Unable to connect to %s" %host
        return e.pgerror
        return e.diag.message_detail
    finally:
        if conn:
            conn.close()
class WebReport(object):
    @cherrypy.expose
    def index(self):
        return GetData()
if __name__ == '__main__':
    cherrypy.server.socket_host = '0.0.0.0'
    cherrypy.quickstart(WebReport())
