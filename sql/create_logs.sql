create table logs(
dt timestamp not null distkey sortkey,
cid varchar(20) not null,
event  varchar(50) not null);
