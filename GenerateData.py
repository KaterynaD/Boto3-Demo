#!/usr/bin/python
"""
GenerateData.py - 03.14.16 Kate Drogaieva

This module generates a support application data set
1. Analysts (HR database for example)
2. Products (Support appliaction database)
3. Cases (Support appliaction database)
4. Logs (can be a text file or a table in NOSQL database)
5. Calendar is generated in the module for simplicity but usually it"s created in Excel and should contain a business specific holidays etc
6. Priority SLA is a file provided by business for analysis of analysts KPIs but we use it to generate logs
The data are saved in the local directories, csv files according to a YAML resource file in this format:
DWDataGeneration:
  StartYear: "2010"
  Num_Analysts: "200"
  Num_Cases: "100"
  Num_Products: "1000"
  Num_Years: "5"
  PrioritySLAFile: "data/HistoricalData/PrioritySLA.csv"
HistoricalDataFiles:
  LocalDir: "data/HistoricalData"
NewDataFiles:
  LocalDir: "data/NewData"

The default resource file name is ProjectResources.yml

PrioritySLA.csv file must already exists. It"s used for random data generation
Here is an example of the data in :
id,name,Assign SLA,Response SLA,Resolve SLA PrioritySLA.csv
1,High,1,3,100
2,Normal,3,24,300
3,Low,24,48,900

The last year data are considered as new data, periodically extracted from an application. 
They are saved in a separate directory

"""
import datetime
import random
from dateutil import parser
from operator import itemgetter
import csv
import os.path
import sys
import yaml
#==============================================================================================================================#
def random_date(start, end):
    """
    This function will return a random datetime between two datetimes
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return start + datetime.timedelta(seconds=random_second)
#******************************************************************************************************************************#
def randId(n):
    """
    This function will return a random ID value (N-XYZ)
    """
    return str(n) +"-" + str(+random.randint(1,10000))
#******************************************************************************************************************************#
def save_list (l,filename):
    """
    This function saves a list in a csv file with quotes)
    """
    with open(filename, "wb") as myfile:
        wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
        wr.writerows(l)
    return
#******************************************************************************************************************************#
def create_skills(skills_par, maxskills):
    """
    This function generate a random number of skills: from 1 to maxskills
    Skill is a letter A or B or C etc
    It does not matter what it represents
    The function checks if a particular skill is already assigned
    """
    skills=skills_par
    for j in xrange(1,random.randint(1,maxskills)):
        skill=chr(random.randint(65,75))
        #we do not want repeating skills
        if skill not in skills:
            skills=skills+","+skill
    return skills.strip(",")
#******************************************************************************************************************************#
def create_analysts (maxnum,year_par):
    """
    This function creates a list of maxnum analysts in this form:
    [Id,Name,Team,Skills,CreatedDate,UpdatedDate]
    The analysts are created between Jan 1 year_par and Dec 31 year_par (CreatedDate)
    An analysts can be updated during the year (UpdatedDate) We do no track the update during the year
    Only year changes do matter in the project
    Name is Personi_year_par 
    Team is a random value 1,2,3,4
    Skills is a random set of max 5 skills - A,B,C etc It does not matter what a particular skill menas
    """
    p=[]
    for i in xrange(maxnum):
        create_d=random_date(datetime.datetime(year_par,1,1,0, 0, 0),datetime.datetime(year_par,12,31,0, 0, 0))
        update_d=create_d+datetime.timedelta(days=random.uniform(0,300))    
        p.append([randId(i), "Person"+str(i)+"_"+str(year_par), "Team"+str(random.randint(1,4)),create_skills("", 5),str(create_d),str(update_d)])
    return p
#******************************************************************************************************************************#
def update_analysts (p, num_changes, next_year_par):
    """
    This function makes 3 changes in the list of analysts:
    1. Update 30% of random analysts changing names, skills and teams
    2. Delete 30% of random analysts
    3. Creates 30% of new analysts
    """
    maxnum=len(p)-1
    #update 30% name,team, skills, Id, CreateDate are still the same, Update is in a next year Jan 1    
    for i in xrange(num_changes):
        j=random.randint(0,maxnum)
        Id=p[j][0]
        create_d=p[j][4]
        update_d=datetime.datetime(next_year_par,1,1,0, 0, 0)
        p[j]=[Id,"Upd"+str(i),"Team"+str(random.randint(1,4)),create_skills(p[j][3], 3),create_d,str(update_d)]
    #delete and add new - 30%
    for i in xrange(num_changes):
        k=random.randint(0,maxnum)
        del p[k]
        create_d=random_date(datetime.datetime(next_year_par,1,1,0, 0, 0),datetime.datetime(next_year_par,12,31,0, 0, 0))
        update_d=create_d+datetime.timedelta(days=random.uniform(0,300))
        p.append([randId(i), "Person"+str(i)+"_"+str(next_year_par), "Team"+str(random.randint(1,4)),create_skills("", 5),str(create_d),str(update_d)])
    return p
#...............................................................................................................................#
def create_cases (maxnum,analysts_par,year_par,products_par):
    """
    This function creates maxnum cases in a list, assigning each to a random analyst from analysts_par and product from products_par
    Each case is created in year_par
    For consistency and simplicity all analysts in analysts_par were created in year_par-1
    Format:
    Id,Priority,CreatedDate,Person,Product
    """
    c=[]
    num_analysts=len(analysts_par)
    num_products=len(products_par)
    for i in xrange(maxnum):
        c.append([randId(i)+"_"+str(year_par),random.randint(1,3),str(random_date(datetime.datetime(year_par,1,1,0, 0, 0),datetime.datetime(year_par,12,31,0, 0, 0))),analysts_par[random.randint(1,num_analysts-1)][0],products_par[random.randint(1,num_products-1)][0]])
    return c
#...............................................................................................................................#
def create_event(caseId_par,prev_event_dt_par,event_name_par,hrs_par):
    """
    It"s an event of a log file for each case
    The event occures in a random time (0, hrs_par) after previous event (prev_event_dt_par)
    The type of events and how hrs_par is defined is described in create_logs function
    Format:
    Event Datetime, case ID, event
    """
    d=prev_event_dt_par+datetime.timedelta(days=random.uniform(0,(hrs_par+random.randint(0,int(hrs_par*2))))/24)
    return [str(d),caseId_par,event_name_par]
#...............................................................................................................................#                               
def create_logs(cases_par):
    """
    The function creates 3 type of events for each case: 
    1. Assigned to analysts (1 event for simplicity)
    2. Response from 1 to 10 events
    3. Close (1 event for simplicity)
    The random time of each event is depend on the previous event time or a case created date for the first event
    It is also defined by a case priority and its SLAs
    Let"s assume we have a file with priority and SLAs from business
    The final goal of the project is to calculate analysts kpis and how they correspond to the SLAs
    The function generates events close to the priority SLAs
    """
    l=[]
    num_cases=len(cases_par)
    for i in xrange(num_cases):
        #base on priority, different slas from PrioritySLA file
        #The header in the file is: id,name,Assign SLA,Response SLA,Resolve SLA
        if cases_par[i][1]==1:
            e=create_event(cases_par[i][0],parser.parse(cases_par[i][2]),"Assigned to analyst",int(PrioritySLA[1][2]))
            l.append(e)
            e=create_event(cases_par[i][0],parser.parse(e[0]),"Response",int(PrioritySLA[1][3]))
            l.append(e)
            for j in xrange(random.randint(0,10)):
                e=create_event(cases_par[i][0],parser.parse(e[0]),"Response",int(PrioritySLA[1][3]))
                l.append(e)
            e=create_event(cases_par[i][0],parser.parse(e[0]),"Close",int(PrioritySLA[1][4]))
            l.append(e)
        elif cases_par[i][1]==2:
            e=create_event(cases_par[i][0],parser.parse(cases_par[i][2]),"Assigned to analyst",int(PrioritySLA[2][2]))
            l.append(e)
            e=create_event(cases_par[i][0],parser.parse(e[0]),"Response",int(PrioritySLA[2][3]))
            l.append(e)
            for j in xrange(random.randint(0,10)):
                e=create_event(cases_par[i][0],parser.parse(e[0]),"Response",int(PrioritySLA[2][3]))
                l.append(e)
            e=create_event(cases_par[i][0],parser.parse(e[0]),"Close",int(PrioritySLA[2][4]))
            l.append(e)
        elif cases_par[i][1]==3:
            e=create_event(cases_par[i][0],parser.parse(cases_par[i][2]),"Assigned to analyst",int(PrioritySLA[3][2]))
            l.append(e)
            e=create_event(cases_par[i][0],parser.parse(e[0]),"Response",int(PrioritySLA[3][3]))
            l.append(e)
            for j in xrange(random.randint(0,10)):
                e=create_event(cases_par[i][0],parser.parse(e[0]),"Response",int(PrioritySLA[3][3]))
                l.append(e)
            e=create_event(cases_par[i][0],parser.parse(e[0]),"Close",int(PrioritySLA[3][4]))
            l.append(e)
    return l
#..............................................................................................................................# 
def create_products(maxnum):
    """
    The function generates a hierarchical data set of products
    organized into Product Lines and Product Groups
    Format:
    Id, Name, Parent Id
    """
    pl=[]
    #Product lines ~ 30% of products
    pl_num=random.randint(3,30*maxnum/100)
    for i in xrange(pl_num):
        pl.append([randId(i),"Product Line"+str(i),""])
    #Product subgroups ~ 60% of products
    ps=[]
    #Product lines ~ 30% of products
    ps_num=random.randint(6,60*maxnum/100)
    for i in xrange(ps_num):
        ps.append([randId(i),"Product Group"+str(i), pl[random.randint(0,pl_num-1)][0]])
    #Products
    p=[]
    for i in xrange(maxnum):
        p.append([randId(i),"Product"+str(i), ps[random.randint(0,ps_num-1)][0]])   
    return [pl,ps,p]
#..............................................................................................................................#
def create_calendar(extend_years):
    c=[]
    """
    The function generate a calendar dimension in the form:
    id,date,day of month,0day of month, mm,0mm,month,mon,quarter,year,day of week, name day of week, abbr name day of week, week of year
    Id is yyyymmdd
    Sunday as the first day of the week 
    It inserts one default record to substitute unknown or null values in fact tables and eluminate outer join in reports
    if a dimension value is arrived later then fact data or does not exist at all  
    we will use -1 in a fact FK field instead of null
    This is only one data set which will use as a dimenaion without any additional SQL transformation
    -1 default record will be added in SQLs for other dimensions
    """
    DefaultDay=datetime.date(year=1900, month=1, day=1) 
    CurrentDay=DefaultDay
    c.append(["-1",CurrentDay,CurrentDay.day,CurrentDay.strftime("%d"),CurrentDay.month,CurrentDay.strftime("%m"),CurrentDay.strftime("%B"),CurrentDay.strftime("%b"),"Q"+str((CurrentDay.month-1)//3+1),CurrentDay.year,CurrentDay.isoweekday(),CurrentDay.strftime("%A"),CurrentDay.strftime("%a"),CurrentDay.strftime("%U")])
    FirstDay=datetime.date(year=start_year, month=1, day=1) 
    LastDay=datetime.date(year=start_year+num_years+extend_years, month=12, day=31) 
    CurrentDay=FirstDay
    #id,date,day of month,0day of month, mm,0mm,month,mon,quarter,year,day of week, name day of week, abbr name day of week, week of year
    #(Sunday as the first day of the week
    while CurrentDay<=LastDay:
        c.append([str(CurrentDay.year)+CurrentDay.strftime("%m")+str(CurrentDay.strftime("%d")),CurrentDay,CurrentDay.day,CurrentDay.strftime("%d"),CurrentDay.month,CurrentDay.strftime("%m"),CurrentDay.strftime("%B"),CurrentDay.strftime("%b"),"Q"+str((CurrentDay.month-1)//3+1),CurrentDay.year,CurrentDay.isoweekday(),CurrentDay.strftime("%A"),CurrentDay.strftime("%a"),CurrentDay.strftime("%U")])
        CurrentDay=CurrentDay+datetime.timedelta(days=1)
    return c
#==============================================================================================================================#
try:
    ResourceFile=sys.argv[1]
except:
    ResourceFile="ProjectResources.yml"
print "A Support  department data warehouse data source set will be created according to the configuration parameters in %s." %ResourceFile
#Read the project resource file
with open(ResourceFile, "r") as f:
    res = yaml.load(f)
try:
    files_dir = res["HistoricalDataFiles"]["LocalDir"]
    last_year_dir=res["NewDataFiles"]["LocalDir"]
    start_year = int(res["DWDataGeneration"]["StartYear"])
    num_analysts = int(res["DWDataGeneration"]["Num_Analysts"])
    num_cases = int(res["DWDataGeneration"]["Num_Cases"])
    num_years = int(res["DWDataGeneration"]["Num_Years"])
    num_products = int(res["DWDataGeneration"]["Num_Products"])
    PrioritySLAFile = res["DWDataGeneration"]["PrioritySLAFile"]
except KeyError:
    sys.exit("Wrong configuration parameters")  
#==============================================================================================================================#
print "Calendar ........"
calendar=create_calendar(5)
save_list(calendar, os.path.join(files_dir,"calendar"+".csv"))
#Priority SLAs
#They are static data usually provided in an Excel file 
#We will use SLA goals to generate real values randomly close to the goals
#The file header (included in the file) is id,name,Assign SLA,Response SLA,Resolve SLA
PrioritySLA=[]
with open(PrioritySLAFile, mode="r") as f:
    reader = csv.reader(f)
    PrioritySLA=map(tuple, reader)
#products
#We need just one product file because it is not historical and new data are just added, hierarchy can be rebuild but no old product ID is deleted and only new added
products=create_products(num_products)
#products[0] - product lines
#products[1] - product subgroups
#products[2] - products
print "Products ........"
save_list(products[0]+products[1]+products[2], os.path.join(files_dir,"products"+".csv"))
#------------------------------------------------------------------------------------------------------------------------------#
#analysts
#we have a file for each year slightly changed
#these data are for SCD type 2
#before we start create cases we need analysts
#for simplicity only analysts added a year before are assigned to the current year cases
#no analysts created in the same year as a case are used
analysts=create_analysts(num_analysts,start_year)
print "analysts year %s........" %start_year
#sort by created date
analysts.sort(key=lambda x: parser.parse(x[4]))
save_list(analysts, os.path.join(files_dir,"analysts_%s.csv" %start_year))
#Changes in the next years
ThirtyPercent=int(30*num_analysts/100)
#data for next years
last_year_cases=[]
last_year_cases_num=0
for y in xrange(1,num_years+1):
    #we start cases in the next year when we already hired analysts
    next_year=start_year+y
    #if it"s the last year we save the data in a separate directory to treat them as "new", periodic data in the opposite to the load historical data all together
    if next_year==start_year+num_years:
        files_dir=last_year_dir
    print
    print "Year=%s" %next_year
    print
    #we can directly assign cases only to products, not to 2nd and 3rd leveles in the product hierarchy
    cases=create_cases(num_cases,analysts,next_year,products[2])
    print
    print "Cases year %s......................" %next_year
    print
    #the last year data will be used not for the initial DW setup but to simulate a periodic update with a new data
    #if it"s a last year we add cases from the prev year to generate logs
    if next_year==start_year+num_years:
        cases=cases+last_year_cases
    #sort by created date
    cases.sort(key=lambda x: parser.parse(x[2]))
    save_list(cases,os.path.join(files_dir,"cases_%s.csv" %next_year))
    #One year before the end
    #do not create logs for 25% of cases
    #the logs will be added in the last year to update prev year cases
    #to simulate periodic update
    if next_year==start_year+num_years-1:
        last_year_cases_num=25*num_cases/100
        for i in xrange(num_cases-1,num_cases-1-last_year_cases_num,-1):
            last_year_cases.append(cases.pop(i))
    #in the very last year logs are created for the current year cases and 25% of prev year cases
    logs=create_logs(cases)
    print
    print "Logs year %s...................................." %next_year
    print
    #sort by created date
    logs.sort(key=lambda x: parser.parse(x[0]))
    save_list(logs,os.path.join(files_dir,"logs_%s.csv" %next_year))
    # next year analysts, will be used for cases in next next year
    next_year_analysts=update_analysts(analysts,ThirtyPercent,next_year)
    print "analysts year %s........"%next_year
    #sort by created date
    next_year_analysts.sort(key=lambda x: parser.parse(x[4]))
    save_list(next_year_analysts, os.path.join(files_dir,"analysts_%s.csv" %next_year))
    analysts=next_year_analysts
