create table #table_name#(
	aid varchar(20) not null distkey sortkey,
	name varchar(50) not null,
	team varchar(30) not null,
	skills varchar(50),
	CreatedDate timestamp not null,
	UpdatedDate timestamp);
