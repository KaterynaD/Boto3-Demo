insert into d_products
(pid, ProductLine, ProductGroup, Product)
select pid
      ,name as ProductLine
	  ,name as ProductGroup
	  ,name as Product
from products
where parent_pid is null
union all
select pg.pid
      ,pl.name as ProductLine
	  ,pg.name as ProductGroup
	  ,pg.name as Product
from products pg,
     (select pid, name from products where parent_pid is null) pl
where pg.parent_pid = pl.pid
union all
select p.pid
      ,pl.name as ProductLine
	  ,pg.name as ProductGroup
	  ,p.name as Product
from products pg,
     (select pid, name from products where parent_pid is null) pl,
	 products p
where pg.parent_pid = pl.pid
  and p.parent_pid=pg.pid;
