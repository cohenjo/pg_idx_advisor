pg_idx_advisor 0.1.2
============

Synopsis
--------
```
create extension pg_idx_advisor;
CREATE EXTENSION

load 'pg_idx_advisor.so';
NOTICE:  IND ADV: plugin loaded
LOAD

explain select * from t where a = 100;
INFO:
** Plan with original indexes **

                                   QUERY PLAN
--------------------------------------------------------------------------------
 Seq Scan on t  (cost=0.00..36.75 rows=11 width=8)
   Filter: (a = 100)

 ** Plan with hypothetical indexes **
 read only, advice, index: create index on t(a)
 Bitmap Heap Scan on t  (cost=4.12..14.79 rows=11 width=8)
   Recheck Cond: (a = 100)
   ->  Bitmap Index Scan on <V-Index>:114699  (cost=0.00..4.11 rows=11 width=0)
         Index Cond: (a = 100)
(9 rows)
```    
Description
-----------

The index advisor uses the virtual index framework/support built into PG
to provide a list of "candidates" for the query engine to choose from.  

The following features are enabled/supported (more to come :) )
Feature list:
- Partial indexes
- CTE
- functional indexes
- text_pattern_ops
- inheritance tables
- composite indexes

Usage
-----

First you must load the library using the 'LOAD' command:
`Load 'g_idx_advisor.so'`
simply run the query with the "explain" - you will see both the original execution plan as well as the new plan with the suggested Virtual/Hypothetical indexes.

Examples:

```
postgres=# explain select c21 from entities_113681518 where c11 = 200;
INFO:
** Plan with original indexes **

                                        QUERY PLAN
------------------------------------------------------------------------------------------
 Seq Scan on entities_113681518  (cost=0.00..10.00 rows=1 width=8)
   Filter: (c11 = 200)

 ** Plan with hypothetical indexes **
 read only, advice, index: create index on entities_113681518(c11)
 Index Scan using <V-Index>:49154 on entities_113681518  (cost=0.00..8.02 rows=1 width=8)
   Index Cond: (c11 = 200)
(7 rows)
```

Works with CTE as well:

```
postgres=# explain with vals as (select c21 from entities_113681518 where c11 = 200 ) select * from vals;
INFO:
** Plan with original indexes **

                                            QUERY PLAN
--------------------------------------------------------------------------------------------------
 CTE Scan on vals  (cost=10.00..10.02 rows=1 width=8)
   CTE vals
     ->  Seq Scan on entities_113681518  (cost=0.00..10.00 rows=1 width=8)
           Filter: (c11 = 200)

 ** Plan with hypothetical indexes **
 read only, advice, index: create index on entities_113681518(c11)
 CTE Scan on vals  (cost=8.02..8.04 rows=1 width=8)
   CTE vals
     ->  Index Scan using <V-Index>:49156 on entities_113681518  (cost=0.00..8.02 rows=1 width=8)
           Index Cond: (c11 = 200)
(11 rows)

```


Support
-------

This library is stored in an open [GitHub repository](https://github.com/cohenjo/pg_idx_advisor). Feel free to fork and contribute! 
Please file bug reports via [GitHub Issues](https://github.com/cohenjo/pg_idx_advisor/issues).

Author
------

[Jony Vesterman Cphen]()

Copyright and License
---------------------

Copyright (c) 2010-2014 Jony Vesterman Cohen.

This module is free software; you can redistribute it and/or modify it under
the [PostgreSQL License](http://www.opensource.org/licenses/postgresql).

Permission to use, copy, modify, and distribute this software and its
documentation for any purpose, without fee, and without a written agreement is
hereby granted, provided that the above copyright notice and this paragraph
and the following two paragraphs appear in all copies.

In no event shall Jony Vesterman Cohen be liable to any party for direct, indirect, 
special, incidental, or consequential damages, including lost profits, 
arising out of the use of this software and its documentation,
even if Jony Vesterman Cohen has been advised of the possibility of such damage.

Jony Vesterman Cohen specifically disclaim any warranties,
including, but not limited to, the implied warranties of merchantability and
fitness for a particular purpose. The software provided hereunder is on an "as
is" basis, and Jony Vesterman Cohen have no obligations to provide
maintenance, support, updates, enhancements, or modifications.
