create table analysts_2010(
	aid char(20) not null distkey sortkey,
	name char(50) not null,
	team varchar(30) not null,
	CreatedDate timestamp not null,
	UpdatedDate timestamp);


create table analysts_2011(
	aid char(20) not null distkey sortkey,
	name char(50) not null,
	team varchar(30) not null,
	CreatedDate timestamp not null,
	UpdatedDate timestamp);
	
create table analysts_2012(
	aid char(20) not null distkey sortkey,
	name char(50) not null,
	team varchar(30) not null,
	CreatedDate timestamp not null,
	UpdatedDate timestamp);
	
create table analysts_2013(
	aid char(20) not null distkey sortkey,
	name char(50) not null,
	team varchar(30) not null,
	CreatedDate timestamp not null,
	UpdatedDate timestamp);
	
create table analysts_2014(
	aid char(20) not null distkey sortkey,
	name char(50) not null,
	team varchar(30) not null,
	CreatedDate timestamp not null,
	UpdatedDate timestamp);
create table cases(
	cid char(20) not null distkey sortkey,
	Priority integer not null,
	CreatedDate timestamp not null,
	aid char(20));
	
create table logs(
dt timestamp not null distkey sortkey,
cid char(20) not null,
event  char(50) not null);
