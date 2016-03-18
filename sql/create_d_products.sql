create table d_products(
	id integer IDENTITY ( 1, 1 ) not null,
	pid varchar(20) not null sortkey,
	ProductLine varchar(100) not null,
	ProductGroup varchar(100) not null,
	Product varchar(100) not null,
	primary key(id));

