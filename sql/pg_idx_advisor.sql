create table index_advisory( reloid	 oid,
	attrs		integer[],
	benefit		real,
	index_size	integer,
	backend_pid	integer,
	timestamp	timestamptz,	
	indcollation int[], -- oidvector
	indclass	int[],
	indoption	int[],
	indexprs	text,
	indpred		text,
	query		text,
	recommendation text);

create index IA_reloid on index_advisory( reloid );
create index IA_backend_pid on index_advisory( backend_pid );
