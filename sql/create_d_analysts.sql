create table d_analysts(
    id integer IDENTITY ( 1, 1 ),
	aid varchar(20) not null sortkey,
	name varchar(50) not null,
	team varchar(30) not null,
	skills varchar(50),
	StartDate timestamp not null,
	EndDate timestamp,
	isActive boolean not null default True,
	primary key(id)
	);
