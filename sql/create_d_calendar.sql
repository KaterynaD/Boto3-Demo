create table d_calendar(
	id integer not null sortkey,
	Dt date not null,
	DayOfMonth integer not null,
	DayOfMonth0 char(2) not null,
	MM integer not null,
	MM0 char(2) not null,
	Month varchar(20) not null,
	Mon char(3) not null,
	Quarter char(2) not null,
	Year integer not null,
	DayOfWeek  integer not null,
	NameDayOfWeek varchar(20) not null,
	AbbrNameDayOfWeek char(3) not null,
	WeekOfYear integer not null,
	primary key(id));
