# pg_idx_advisor
=================

[![PGXN version](https://badge.fury.io/pg/pg_idx_advisor.svg)](https://badge.fury.io/pg/pg_idx_advisor)

A PostgreSQL extension to analyze queries and give indexing advice.

## Note: This is no longer updated - please see: https://github.com/HypoPG/hypopg

## Introduction ##
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

## Installation ##
    make
    make install

If you encounter an error such as:

    make: pg_config: Command not found

Be sure that you have `pg_config` installed and in your path. If you used a
package management system such as RPM to install PostgreSQL, be sure that the
`-devel` package is also installed. If necessary tell the build process where
to find it:

    env PG_CONFIG=/path/to/pg_config make && make installcheck && make install



## Usage ##
First Create the extension `create extension pg_idx_advisor;` it will create the requires table to store query recommendations.
Then you must load the library using the 'LOAD' command: `Load 'pg_idx_advisor.so'`
That's it - you are ready to be advised on your first query.
simply run the query with the "explain" keyword - you will see both the original execution plan as well as the new plan with the suggested Virtual/Hypothetical indexes.

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


Dependencies
------------
The `pg_idx_advisor` extension has no dependencies other than PostgreSQL.

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
