create table cases(
	cid varchar(20) not null distkey sortkey,
	Priority integer not null,
	CreatedDate timestamp not null,
	aid varchar(20),
	pid varchar(20));
