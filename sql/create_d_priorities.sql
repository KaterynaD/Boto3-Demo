create table d_priorities(
	id integer not null,
	name varchar(20) not null ,
	AssignSLA integer not null,
	ResponseSLA integer not null,
	ResolveSLA integer not null,
	primary key(id)
	)
	diststyle all;
