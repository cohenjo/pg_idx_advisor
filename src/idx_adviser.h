/*!-------------------------------------------------------------------------
 *
 * \file idx_adviser.h
 * \brief     Prototypes for idx_adviser.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef IDX_ADVISOR_H
#define IDX_ADVISOR_H 1


#include "postgres.h"

#include "nodes/print.h"
#include "parser/parsetree.h"
#include "catalog/namespace.h"
#include "executor/executor.h"
#include "utils.h"

/*! \struct IndexCandidate
 * \brief A struct to represent an index candidate.
 * contains all the information needed to create the virtual index
 */
typedef struct {

	Index		varno;					/**< index into the rangetable */
	Index		varlevelsup;			/**< points to the correct rangetable */
	int8		ncols;					/**< number of indexed columns */
	Oid		vartype[INDEX_MAX_KEYS];/**< type of the column(s) */
	AttrNumber	varattno[INDEX_MAX_KEYS];/**< attribute number of the column(s) */
	char*       	varname[INDEX_MAX_KEYS];/**< attribute name */
	Oid		op_class[INDEX_MAX_KEYS];			/* the field op class family */
	Oid		collationObjectId[INDEX_MAX_KEYS];	/* the field collation */
	List *		attList;				/**< list of IndexElem's - describe each parameter */
	Oid		reloid;					/**< the table oid */
	char*       	erefAlias;              /**< hold rte->eref->aliasname */
	Oid		idxoid;				    /**< the virtual index oid */
	BlockNumber	pages;					/**< the estimated size of index */
	double		tuples;					/**< number of index tuples in index */
	bool		idxused;				/**< was this used by the planner? */
	float4		benefit;				/**< benefit made by using this cand */
	bool		inh;					/**< does the RTE allow inheritance */
	Oid	 	parentOid;				/**< the parent table oid */
	Oid		amOid;
} IndexCandidate;

/*!
 * \brief A struct to keep the relation clause until we create the relevant candidates.
 */
typedef struct {
    Oid         reloid;					/**< the table oid */
    char*       erefAlias;              /**< hold rte->eref->aliasname */
    List*       predicate;              /**< the predicates used for the partial indexs */
} RelClause;

typedef struct {
    List*   predicate;                  /**< the predicates used for the partial indexs */
    List*   candidates;                 /**< list of candidates init to NIL; */
} QueryContext;

typedef struct {
    List*   opnos;                  /**< list of supported b-tree operations */
    List*   ginopnos;                 /**< list of supported gin operations */
    List*   gistopnos;                 /**< list of supported gist operations */
} OpnosContext;

typedef struct
{
	List* candidates;
	OpnosContext* context;
	List* rangeTableStack;
} ScanContext;

extern void _PG_init(void);
extern void _PG_fini(void);

#define compile_assert(x)	extern int	_compile_assert_array[(x)?1:-1]

/* *****************************************************************************
 * DEBUG Level	: Information dumped
 * ------------   ------------------
 *	DEBUG1		: code level logging. What came in and what went out of a
 *					function. candidates generated, cost estimates, etc.
 *	DEBUG2		: DEBUG1 plus : Profiling info. Time consumed in each of the
 *					major functions.
 *	DEBUG3		: Above plus : function enter/leave info.
 * ****************************************************************************/

#define DEBUG_LEVEL_COST	(log_min_messages >= DEBUG1)
#define DEBUG_LEVEL_PROFILE	(log_min_messages >= DEBUG2)
#define DEBUG_LEVEL_CANDS	(log_min_messages >= DEBUG3)

/* Index Adviser output table */
#define IDX_ADV_TABL "index_advisory"

/* IDX_ADV_TABL does Not Exist */
#define IDX_ADV_ERROR_NE	"relation \""IDX_ADV_TABL"\" does not exist."

/* IDX_ADV_TABL is Not a Table or a View */
#define IDX_ADV_ERROR_NTV	"\""IDX_ADV_TABL"\" is not a table or view."

#define IDX_ADV_ERROR_DETAIL												   \
	"Index Adviser uses \""IDX_ADV_TABL"\" table to store it's advisory. You"  \
	" should have INSERT permissions on a table or an (INSERT-able) view named"\
	" \""IDX_ADV_TABL"\". Also, make sure that you are NOT running the Index"  \
	" Adviser under a read-only transaction."

#define IDX_ADV_ERROR_HINT													   \
	"Please create the \""IDX_ADV_TABL"\" table."


#endif   /* IDX_ADVISOR_H */
