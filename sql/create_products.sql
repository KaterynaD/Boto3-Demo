create table products(
	pid varchar(20) not null distkey sortkey,
	Name varchar(100) not null,
	parent_pid varchar(20));

