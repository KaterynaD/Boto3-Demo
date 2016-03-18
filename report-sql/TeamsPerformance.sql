select '<b>SLA</b>' team
,(select round(assignsla,2) from d_priorities where id=1) priority_1
,(select round(assignsla,2) from d_priorities where id=2) priority_2
,(select round(assignsla,2) from d_priorities where id=3) priority_3
union all 
select '<b>'+team+'</b>' team
,round(max(priority_1),2) priority_1
,round(max(priority_2),2) priority_2
,round(max(priority_3),2) priority_3
from
(
select team
,case priority_id
when 1 then avg_assigned
end priority_1
,case priority_id
when 2 then avg_assigned
end priority_2
,case priority_id
when 3 then avg_assigned
end priority_3
from
(
select 
d.team
,f.priority_id
,avg(datediff(sec,f.CreatedDate,f.AssignedDate)/60.0/60.0) avg_assigned
from f_cases f,
d_analysts d
where f.analyst_id=d.id
and f.AssignedDate is not null
and f.createdDt_id>20100101
group by d.team
, f.priority_id
)
)
group by team
order by team

