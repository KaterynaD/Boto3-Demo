-- Start a new transaction
begin transaction;
--if there is a change in historical fields, close the current active record and create a new one using insert statement later
update d_analysts
 set isactive=False
    ,enddate=(select dateadd(sec,-1,an.updateddate) from analysts an where an.aid=d_analysts.aid)
 where id  in (select a1.id from analysts an, d_analysts a1 where a1.isactive=True and an.aid=a1.aid and (a1.team<>an.team or a1.skills<>an.skills));

 --update data in NON historical fields
update d_analysts
 set name=analysts.name
 from analysts
 where analysts.aid=d_analysts.aid;
 
 --insert new records
insert into d_analysts (aid, name, team, skills, StartDate)
select aid, name, team, skills, updateddate from analysts
where aid not in (select aid from d_analysts where isactive=True);

commit;
end transaction;
