set client_min_messages to log;

load '$libdir/plugins/pg_idx_advisor.so';


drop table if exists t, t1;
create table t( a int, b int );
create table t1( a int, b int );


explain select * from t where a = 100;

explain select * from t where b = 100;

explain select * from t where a = 100 and b = 100;

explain select * from t where a = 100 or b = 100;

/* let's do some sensible join over these two tables */;
explain select * from t, t1 where t.a = 100 and t1.a = 100 and t1.b = 100;

explain with acte as (select * from t where a = 200) select * from acte;

select * from index_advisory;
