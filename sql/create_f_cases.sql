create table f_cases(
	id integer IDENTITY ( 1, 1 ) not null, --PK
	cid varchar(20) not null , --case natural key
	priority_id integer not null, --d_priority FK
	createdDt_id integer not null, --d_calendar FK
	createdDate timestamp not null, --fact, to calculate SLA
	aid varchar(20) , --analyst natural key
	analyst_id integer , --d_analysts FK
	pid varchar(20) , --product natural key
	product_id integer, --d_products FK
	assignedDt_id integer,--d_calendar FK
	responseDt_id integer,--d_calendar FK
	resolveDt_id integer,--d_calendar FK
	assignedDate timestamp, --fact, to calculate SLA
	responseDate timestamp, --fact, to calculate SLA
	resolveDate timestamp, --fact, to calculate SLA
	primary key(id),
	foreign key(priority_id) references d_priorities(id),
	foreign key(createdDt_id) references d_calendar(id),
	foreign key(analyst_id) references d_analysts(id),
	foreign key(product_id) references d_products(id),
	foreign key(assignedDt_id) references d_calendar(id),
	foreign key(responseDt_id) references d_calendar(id),
	foreign key(resolveDt_id) references d_calendar(id)
	)
	compound sortkey (createdDt_id,analyst_id,priority_id,product_id);
