begin transaction;

--insert new records
insert into d_analysts (aid, name, team, skills, StartDate)
select aid, name, team, skills,'01.01.2010 00:00:00' from analysts_2010
where aid not in (select aid from d_analysts where isactive=True);

--next year data source
--if there is a change in historical fields, close the current active record and create a new one using insert statement later
update d_analysts
 set isactive=False
    ,enddate='12.31.2010 23:59:59'
 where id  in (select a1.id from analysts_2011 an, d_analysts a1 where a1.isactive=True and an.aid=a1.aid and (a1.team<>an.team or a1.skills<>an.skills));
 
--update data in NON historical fields
update d_analysts
 set name=analysts_2011.name
 from analysts_2011
 where analysts_2011.aid=d_analysts.aid;
 
--insert new records	
insert into d_analysts (aid, name, team, skills, StartDate)
select aid, name, team, skills, '01.01.2011 00:00:00' from analysts_2011
where aid not in (select aid from d_analysts where isactive=True);

--next year data source
--if there is a change in historical fields, close the current active record and create a new one using insert statement later
update d_analysts
 set isactive=False
    ,enddate='12.31.2011 23:59:59'
 where id  in (select a1.id from analysts_2012 an, d_analysts a1 where a1.isactive=True and an.aid=a1.aid and (a1.team<>an.team or a1.skills<>an.skills));

 --update data in NON historical fields
update d_analysts
 set name=analysts_2012.name
 from analysts_2012
 where analysts_2012.aid=d_analysts.aid;
 
 --insert new records
insert into d_analysts (aid, name, team, skills, StartDate)
select aid, name, team, skills, '01.01.2012 00:00:00' from analysts_2012
where aid not in (select aid from d_analysts where isactive=True);

--next year data source
--if there is a change in historical fields, close the current active record and create a new one using insert statement later
update d_analysts
 set isactive=False
    ,enddate='12.31.2012 23:59:59'
 where id  in (select a1.id from analysts_2013 an, d_analysts a1 where a1.isactive=True and an.aid=a1.aid and (a1.team<>an.team or a1.skills<>an.skills));

--update data in NON historical fields
update d_analysts
 set name=analysts_2013.name
 from analysts_2013
 where analysts_2013.aid=d_analysts.aid;
 
 --insert new records
insert into d_analysts (aid, name, team,skills, StartDate)
select aid, name, team, skills, '01.01.2013 00:00:00' from analysts_2013
where aid not in (select aid from d_analysts where isactive=True);

--next year data source
--if there is a change in historical fields, close the current active record and create a new one using insert statement later
update d_analysts
 set isactive=False
    ,enddate='12.31.2013 23:59:59'
 where id  in (select a1.id from analysts_2014 an, d_analysts a1 where a1.isactive=True and an.aid=a1.aid and (a1.team<>an.team or a1.skills<>an.skills));

 --update data in NON historical fields
update d_analysts
 set name=analysts_2014.name
 from analysts_2014
 where analysts_2014.aid=d_analysts.aid;
 
 --insert new records
insert into d_analysts (aid, name, team, skills, StartDate)
select aid, name, team, skills, '01.01.2014 00:00:00' from analysts_2014
where aid not in (select aid from d_analysts where isactive=True);

commit;
end transaction;
