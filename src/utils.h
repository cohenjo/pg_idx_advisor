
/*!-------------------------------------------------------------------------
 *
 * \file index_advisor.h
 * \brief     Prototypes for indexadvisor.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef UTILS_H
#define UTILS_H 1


#include "postgres.h"

#include <ctype.h>
#include <math.h>

#include "access/gin.h"
#include "access/sysattr.h"
#if PG_VERSION_NUM >= 90300
#include "access/htup_details.h"
#else
#include "access/htup.h"
#endif
#include "catalog/index.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_type.h"
#include "catalog/pg_opclass.h"
#include "executor/executor.h"
#include "mb/pg_wchar.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/plancat.h"
#include "optimizer/predtest.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/date.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/nabstime.h"
#include "utils/pg_locale.h"
#include "utils/rel.h"
#include "utils/selfuncs.h"
#include "utils/spccache.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/tqual.h"
#include "utils/typcache.h"
#include "catalog/pg_operator.h"



typedef struct {   
    char    partQlauseCol[80];        /**< holds partial clause column names, seperated by ',' */
	bool	text_pattern_ops;
	int		composit_max_cols;
	bool	read_only;
} Configuration;




Configuration* parse_config_file(const char *filename, const char *cols, const bool text_pattern_ops, const int composit_max_cols, const bool read_only);
//Configuration* parse_config_file(const char *file_name);


void get_opclass_name(Oid opclass, Oid actual_datatype, StringInfo buf);
extern double var_eq_cons(VariableStatData *vardata, Oid operator,Datum constval, bool constisnull,bool varonleft);
//void dump_trace();
List* create_operator_list(char *SupportedOps);
#define BOOL_FMT(bool_expr) (bool_expr) ? "true" : "false"

#endif   /* UTILS_H */
