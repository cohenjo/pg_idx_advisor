/*!-------------------------------------------------------------------------
 *
 * \file idx_adviser.c
 * \brief Plugin to analyze and give indexing advice.
 *
 *
 *
 *
 * Noted projects:
 * ================
 * pg_adviser - <https://github.com/gurjeet/pg_adviser>
 * Hypothetical indexes in PostgreSQL	<http://hypotheticalind.sourceforge.net/>
 *
 * Project History:
 * ================
 * 10/06/2013   Jony Cohen      Initial Version compile & run against PostgreSQL 9.2.4
 * 15/09/2013   Jony Cohen      added functionality to support partial & functional indexes.
 * 15/02/2015	Jony Cohen	    Support for PostgreSQL 9.4
 * 22/02/2015	Jony Cohen	    Expression tree walker - Increase the code stability for newer versions. (Support CTE etc.)
 *
 *-------------------------------------------------------------------------
 */

/* ------------------------------------------------------------------------
 * Includes 
 * ------------------------------------------------------------------------
 */
//#include <sys/time.h>

#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/itup.h"
#include "access/nbtree.h"
#include "access/xact.h"
#include "idx_adviser.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "executor/execdesc.h"
#include "executor/spi.h"
#include "fmgr.h"									   /* for PG_MODULE_MAGIC */
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "nodes/print.h"
#include "optimizer/planner.h"
#include "optimizer/plancat.h"
#include "parser/parse_coerce.h"
#include "parser/parsetree.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "tcop/dest.h"
#include "tcop/tcopprot.h"
#include "utils.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/selfuncs.h"

/* mark this dynamic library to be compatible with PG as of PG 8.2 */
PG_MODULE_MAGIC;

/* *****************************************************************************
 * Function Declarations
 * ****************************************************************************/

static bool index_candidates_walker (Node *root, ScanContext *context);
static List* scan_query(	const Query* const query,
				List* const opnos,
				List* rangeTableStack );

static List* scan_generic_node(	const Node* const root,
				List* const opnos,
				List* const rangeTableStack );

static List* scan_group_clause(	List* const groupList,
				List* const targtList,
				List* const opnos,
				List* const rangeTblStack );

static List* scan_targetList(   List* const targetList,
                                List* const opnos,
                                List* const rangeTableStack );


static List* build_composite_candidates( List* l1, List* l2 );

static List* remove_irrelevant_candidates( List* candidates );
static void tag_and_remove_candidates(Cost startupCostSaved, 
				  Cost totalCostSaved, 
				  PlannedStmt *new_plan,
				  const Node* const head,
				  List* const candidates);
static void mark_used_candidates(	const Node* const plan,
					List* const candidates );

static int compare_candidates(	const IndexCandidate* c1,
				const IndexCandidate* c2 );

static List* merge_candidates( List* l1, List* l2 );
static List* expand_inherited_candidates(List* list1);
static void expand_inherited_rel_clauses();


static List* create_virtual_indexes( List* candidates );
static void drop_virtual_indexes( List* candidates );

static List* get_rel_clauses(List* table_clauses, Oid reloid, char* erefAlias);
static ListCell* get_rel_clausesCell(List* table_clauses, Oid reloid, char* erefAlias);
static Expr* makePredicateClause(OpExpr* root,Const* constArg, Var* VarArg);
static List *build_index_tlist(PlannerInfo *root, IndexOptInfo *index,
                                  Relation heapRelation);

static void store_idx_advice( List* candidates, ExplainState * 	es );

static void log_candidates( const char* text, List* candidates );

/* function used for estimating the size of virtual indexes */
static BlockNumber estimate_index_pages(Oid rel_oid, Oid ind_oid );

static PlannedStmt* planner_callback(	Query*			query,
					int				cursorOptions,
					ParamListInfo	boundParams);

static void ExplainOneQuery_callback(	Query*	query,  IntoClause *into,
					ExplainState*	stmt,
					const char*	queryString,
					ParamListInfo	params); 

static void get_relation_info_callback(	PlannerInfo*	root,
					Oid				relationObjectId,
					bool			inhparent,
					RelOptInfo*		rel);

static const char* explain_get_index_name_callback( Oid indexId );

static PlannedStmt* index_adviser(	Query*			query,
					int				cursorOptions,
					ParamListInfo	boundParams,
					PlannedStmt*	actual_plan,
					ExplainState * 	es,
					bool			doingExplain);

static void resetSecondaryHooks(void);
static bool is_virtual_index( Oid oid, IndexCandidate** cand_out );

/* ------------------------------------------------------------------------
 * Global Parameters
 * ------------------------------------------------------------------------
 */

/*! global list of index candidates. */
static List* index_candidates;
/* Need this to store the predicate for the partial indexes. */
//static QueryContext* context;

/*! Need this to store table clauses until they are filled in the candidates*/
static List* table_clauses;

/*! Holds the advisor configuration */
static bool idxadv_read_only;
static bool idxadv_text_pattern_ops;
static char *idxadv_columns;
static char *idxadv_schema;
static int	idxadv_composit_max_cols;


/*! Global variable to hold a value across calls to mark_used_candidates() */
static PlannedStmt* plannedStmtGlobal;
//static char *envVar;


/** parameters to store the old hooks */
static planner_hook_type prev_planner = NULL;
static ExplainOneQuery_hook_type prev_ExplainOneQuery = NULL;


/* ------------------------------------------------------------------------
 * implementations: index adviser
 * ------------------------------------------------------------------------
 */

/* PG calls this func when loading the plugin */
void _PG_init(void)
{
	elog(DEBUG1,"IND ADV: load parameters");
	/* Read parameters */
	DefineCustomStringVariable("index_adviser.cols",
		"comma separated list of column names to be used in partial indexes",
							NULL,
							&idxadv_columns,
							"entity_type_id,is_deleted",
							PGC_SUSET,
							0,
							NULL,
							NULL,
							NULL);
	DefineCustomStringVariable("index_adviser.schema",
		"index advisory recommendation schema",
							NULL,
							&idxadv_schema,
							"public",
							PGC_SUSET,
							0,
							NULL,
							NULL,
							NULL);
	elog(DEBUG1,"IND ADV: load parameters");
	DefineCustomBoolVariable("index_adviser.read_only",
	   "disables insertion of recommendations to the advisory table - only prints to screen",
							NULL,
							&idxadv_read_only,
							false,
							PGC_SUSET,
							0,
							NULL,
							NULL,
							NULL);
	elog(DEBUG1,"IND ADV: load parameters");
	DefineCustomBoolVariable("index_adviser.text_pattern_ops",
	   "allows creation of text indexes with text_pattern_ops",
							NULL,
							&idxadv_text_pattern_ops,
							true,
							PGC_SUSET,
							0,
							NULL,
							NULL,
							NULL);
	DefineCustomIntVariable("index_adviser.composit_max_cols",
							"max number of columns to use in composite indexes.",
							NULL,
							&idxadv_composit_max_cols,
							3,
							1,
							INT_MAX,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	elog(DEBUG1,"IND ADV: loaded parameters");
	/* Install hookds. */
	prev_ExplainOneQuery = ExplainOneQuery_hook;
	ExplainOneQuery_hook = ExplainOneQuery_callback;
	prev_planner = planner_hook;
	planner_hook = planner_callback;

	/* We dont need to reset the state here since the contrib module has just been
	 * loaded; FIXME: consider removing this call.
	 */
	resetSecondaryHooks();

	elog( NOTICE , "IND ADV: plugin loaded" );
}

/* PG calls this func when un-loading the plugin (if ever) */
void _PG_fini(void)
{
    /* Uninstall hooks. */
	planner_hook = prev_planner;
	ExplainOneQuery_hook = prev_ExplainOneQuery;

	resetSecondaryHooks();

	elog( NOTICE , "IND ADV: plugin unloaded." );
}

/* Make sure that Cost datatype can represent negative values */
compile_assert( ((Cost)-1) < 0 );

/* As of now the order-by and group-by clauses use the same C-struct.
 * A rudimentary check to confirm this:
 */
compile_assert( sizeof(*((Query*)NULL)->groupClause) == sizeof(*((Query*)NULL)->sortClause) );

/**
 * index_adviser
 *		Takes a query and the actual plan generated by the standard planner for
 * that query. It then creates virtual indexes, using the columns used in the
 * query, and asks the standard planner to generate a new plan for the query.
 *
 *     If the new plan appears to be cheaper than the actual plan, it
 * saves the information about the virtual indexes that were used by the
 * planner in an advisory table.
 *
 *     If it is called by the Explain-hook, then it returns the newly generated
 * plan (allocated in caller's memory context), so that ExplainOnePlan() can
 * generate and send a string representation of the plan to the log or the client.
 */

/* TODO: Make recursion suppression more bullet-proof. ERRORs can leave this indicator on. */
static int8	SuppressRecursion = 0;		  /* suppress recursive calls */

static PlannedStmt* index_adviser(	Query*			queryCopy,
				int				cursorOptions,
				ParamListInfo	boundParams,
				PlannedStmt		*actual_plan,
				ExplainState * 	es,
				bool			doingExplain)
{
	bool		saveCandidates = false;
	int			i;
	ListCell	*cell;
	List*       opnos = NIL;			  /* contains all vaild operator-ids */
	List*		candidates = NIL;				  /* the resulting candidates */

	Cost		actualStartupCost;
	Cost		actualTotalCost;
	Cost		newStartupCost;
	Cost		newTotalCost;
	Cost		startupCostSaved;
	Cost		totalCostSaved;
	float4		startupGainPerc;							/* in percentages */
	float4		totalGainPerc;

	ResourceOwner	oldResourceOwner;
	PlannedStmt		*new_plan;
	MemoryContext	outerContext;
	

	char *SupportedOps[] = { "=", "<", ">", "<=", ">=", "~~", }; /* Added support for LIKE ~~ */

	
	elog( DEBUG3, "IND ADV: Entering" );
	

	/* We work only in Normal Mode, and non-recursively; that is, we do not work
	 * on our own DML.
	 */
	if( IsBootstrapProcessingMode() || SuppressRecursion++ > 0 )
	{
		new_plan = NULL;
		goto DoneCleanly;
	}

	/* Remember the memory context; we use it to pass interesting data back. */
	outerContext = CurrentMemoryContext;

	/* reset these globals; since an ERROR might have left them unclean */	
	index_candidates = NIL;
	table_clauses = NIL;
	

	/* get the costs without any virtual index */
	actualStartupCost	= actual_plan->planTree->startup_cost;
	actualTotalCost		= actual_plan->planTree->total_cost;
	elog( DEBUG2 , "IND ADV: actual plan costs: %lf .. %lf",actualStartupCost,actualTotalCost);

	/* create list containing all operators supported by the index advisor */
	for( i=0; i < lengthof(SupportedOps); ++i )
	{
		FuncCandidateList   opnosResult;

		List* supop = list_make1( makeString( SupportedOps[i] ) );

		/* 
		 * collect operator ids into an array.
		 */
		for(	opnosResult = OpernameGetCandidates( supop, '\0' 
#if PG_VERSION_NUM >= 90400
                                                           , true
#endif
                                                           );
				opnosResult != NULL;
				opnosResult = lnext(opnosResult) )
		{
			//elog(DEBUG2, "opno: %d, %s",opnosResult->oid ,SupportedOps[i]);
			opnos = lappend_oid( opnos, opnosResult->oid );
		}

		/* free the Value* (T_String) and the list */
		pfree( linitial( supop ) );
		list_free( supop );
	}

	elog( DEBUG3, "IND ADV: Generate index candidates" );
	/* Generate index candidates */
	candidates = scan_query( queryCopy, opnos, NULL );

	/* the list of operator oids isn't needed anymore */
	list_free( opnos );

	if (list_length(candidates) == 0)
		goto DoneCleanly;

	log_candidates( "Generated candidates", candidates );
	elog( DEBUG3, "IND ADV: remove all irrelevant candidates" );
	/* remove all irrelevant candidates */
	candidates = remove_irrelevant_candidates( candidates );

	if (list_length(candidates) == 0)
		goto DoneCleanly;

	log_candidates( "Relevant candidates", candidates );
	/*
	 * We need to restore the resource-owner after RARCST(), only if we are
	 * called from the executor; but we do it all the time because,
	 * (1) Its difficult to determine if we are being called by the executor.
	 * (2) It is harmless.
	 * (3) It is not much of an overhead!
	 */
	oldResourceOwner = CurrentResourceOwner;

	/*
	 * Setup an SPI frame around the BeginInternalSubTransaction() and
	 * RollbackAndReleaseCurrentSubTransaction(), since xact.c assumes that
	 * BIST()/RARCST() infrastructure is used only by PL/ interpreters (like
	 * pl/pgsql), and hence it calls AtEOSubXact_SPI(), and that in turn frees
	 * all the execution context memory of the SPI (which _may_ have invoked the
	 * adviser). By setting up our own SPI frame here, we make sure that
	 * AtEOSubXact_SPI() frees this frame's memory.
	 */
	elog( DEBUG1, "About to call SPI connect - push SPI first");
        //SPI_push();
	elog( DEBUG1, "SPI connection start - TODO FIX THIS!!");
	if( SPI_connect() != SPI_OK_CONNECT )
	{
		elog( WARNING, "IND ADV: SPI_connect() call failed - pre virtual index creation." );		
		goto DoneCleanly;
	}

	/*
	 * DO NOT access any data-structure allocated between BEGIN/ROLLBACK
	 * transaction, after the ROLLBACK! All the memory allocated after BEGIN is
	 * freed in ROLLBACK.
	 */
	 elog( DEBUG1, "Start internal sub transaction");
	BeginInternalSubTransaction( "index_adviser" );

	elog( DEBUG1, "now create the virtual indexes ");
	/* now create the virtual indexes */
	candidates = create_virtual_indexes( candidates );

	/* update the global var */
	index_candidates = candidates;

	/*
	 * Setup the hook in the planner that injects information into base-tables
	 * as they are prepared
	 */
	get_relation_info_hook = get_relation_info_callback;

	elog( DEBUG1, "IDX ADV: do re-planning using virtual indexes" );
	/* do re-planning using virtual indexes */
	/* TODO: is the plan ever freed? */
	
	new_plan = standard_planner(queryCopy, cursorOptions, boundParams);
	
	elog( DEBUG1, "IND ADV: release the hook" );
	/* reset the hook */
	get_relation_info_hook = NULL;

	elog( DEBUG1, "IND ADV: remove the virtual-indexes" );
	/* remove the virtual-indexes */
	drop_virtual_indexes( candidates );
	
	newStartupCost	= new_plan->planTree->startup_cost;
	newTotalCost	= new_plan->planTree->total_cost;
    elog( DEBUG1 , "IND ADV: new plan costs: %lf .. %lf ",newStartupCost,newTotalCost);

	elog( DEBUG1, "IND ADV: calculate the cost benefits" );
	/* calculate the cost benefits */
	startupGainPerc =
		actualStartupCost == 0 ? 0 :
			(1 - newStartupCost/actualStartupCost) * 100;

	totalGainPerc =
		actualTotalCost == 0 ? 0 :
			(1 - newTotalCost/actualTotalCost) * 100;

	startupCostSaved = actualStartupCost - newStartupCost;

	totalCostSaved = actualTotalCost - newTotalCost;

	
	tag_and_remove_candidates(startupCostSaved, totalCostSaved, new_plan, (Node*)new_plan->planTree, candidates);
/*	
	if( startupCostSaved >0 || totalCostSaved > 0 )
	{
	
		plannedStmtGlobal = new_plan;

		//elog_node_display( DEBUG4, "plan (using Index Adviser)",(Node*)new_plan->planTree, true );

		mark_used_candidates(, candidates );

		plannedStmtGlobal = NULL;
	}
*/
	

	/* update the global var */
	index_candidates = candidates;

	elog( DEBUG2, "IND ADV: log the candidates used by the planner" );
	/* log the candidates used by the planner */
	log_candidates( "Used candidates", candidates );

	if( list_length( candidates ) > 0 )
		saveCandidates = true;

	/* calculate the share of cost saved by each index */
	if( saveCandidates )
	{
		int8 totalSize = 0;
		IndexCandidate *cand;

		foreach( cell, candidates )
			totalSize += ((IndexCandidate*)lfirst( cell ))->pages;

		foreach( cell, candidates )
		{
			cand = (IndexCandidate*)lfirst( cell );

			elog( DEBUG2, "IND ADV: benefit: saved: %f, pages: %d, size: %d", totalCostSaved,cand->pages,totalSize);
			cand->benefit = (float4)totalCostSaved
							* ((float4)cand->pages/totalSize);
		}
	}

	elog( DEBUG2, "IND ADV: Print the new plan if debugging" );
	/* Print the new plan if debugging. */
	if( saveCandidates && Debug_print_plan )
		elog_node_display( DEBUG2, "plan (using Index Adviser)",
							new_plan, Debug_pretty_print );

	/* If called from the EXPLAIN hook, make a copy of the plan to be passed back */
	if( saveCandidates && doingExplain )
	{
		MemoryContext oldContext = MemoryContextSwitchTo( outerContext );

		new_plan = copyObject( new_plan );

		MemoryContextSwitchTo( oldContext );
	}
	else
	{
		/* TODO: try to free the new plan node */
		new_plan = NULL;
	}
	/*
	 * Undo the metadata changes; for eg. pg_depends entries will be removed
	 * (from our MVCC view).
	 *
	 * Again: DO NOT access any data-structure allocated between BEGIN/ROLLBACK
	 * transaction, after the ROLLBACK! All the memory allocated after BEGIN is
	 * freed in ROLLBACK.
	 */
	RollbackAndReleaseCurrentSubTransaction();

	/* restore the resource-owner */
	CurrentResourceOwner = oldResourceOwner;

	elog( DEBUG1, "SPI connection finish");
	if( SPI_finish() != SPI_OK_FINISH )
		elog( WARNING, "IND ADV: SPI_finish failed." );

	elog( DEBUG1, "IND ADV: save the advice into the table" );
	/* save the advise into the table */
	if( saveCandidates )
	{
		/* catch any ERROR */
		PG_TRY();
		{
			elog( DEBUG1, "IND ADV: pre-save the advise into the table" );
			store_idx_advice(candidates, es);
			elog( DEBUG1, "IND ADV: post-save the advise into the table" );
		}
		PG_CATCH();
		{
			/* reset our 'running' state... */
			--SuppressRecursion;

			/*
			 * Add a detailed explanation to the ERROR. Note that these function
			 * calls will overwrite the DETAIL and HINT that are already
			 * associated (if any) with this ERROR. XXX consider errcontext().
			 */
			errdetail( IDX_ADV_ERROR_DETAIL );
			errhint( IDX_ADV_ERROR_HINT );

			/* ... and re-throw the ERROR */
			PG_RE_THROW();
		}
		PG_END_TRY();

	}

	/* free the candidate-list */
	elog( DEBUG3, "IND ADV: Deleting candidate list." );
	if( !saveCandidates || !doingExplain )
	{
		foreach( cell, index_candidates )
			pfree( (IndexCandidate*)lfirst( cell ) );

		list_free( index_candidates );
		index_candidates = NIL;

		foreach( cell, table_clauses )
            pfree( (RelClause*)lfirst( cell ) );

        list_free( table_clauses );
        table_clauses = NIL;
	}

	elog( DEBUG3, "IND ADV: Done." );

	/* emit debug info */
	elog( DEBUG1, "IND ADV: old cost %.2f..%.2f", actualStartupCost,
													actualTotalCost );
	elog( DEBUG1, "IND ADV: new cost %.2f..%.2f", newStartupCost, newTotalCost);
	elog( DEBUG1, "IND ADV: cost saved %.2f..%.2f, these are %lu..%lu percent",
					startupCostSaved,
					totalCostSaved,
					(unsigned long)startupGainPerc,
					(unsigned long)totalGainPerc );


DoneCleanly:
	/* allow new calls to the index-adviser */
	--SuppressRecursion;

	elog( DEBUG3, "IDX ADV: EXIT" );
	return doingExplain && saveCandidates ? new_plan : NULL;
}

/*
 * This callback is registered immediately upon loading this plugin. It is
 * responsible for taking over control from the planner.
 *
 *     It calls the standard planner and sends the resulting plan to
 * index_adviser() for comparison with a plan generated after creating
 * hypothetical indexes.
 */
static PlannedStmt* planner_callback(	Query*			query,
					int				cursorOptions,
					ParamListInfo	boundParams)
{
	Query	*queryCopy;
	PlannedStmt *actual_plan;
	PlannedStmt *new_plan;

	resetSecondaryHooks();

	/* TODO : try to avoid making a copy if the index_adviser() is not going
	 * to use it; Index Adviser may not use the query copy at all if we are
	 * running in BootProcessing mode, or if the Index Adviser is being called
	 * recursively.
	 */

	elog( DEBUG3 , "planner_callback: enter");
	/* planner() scribbles on it's input, so make a copy of the query-tree */
	queryCopy = copyObject( query );

	/* Generate a plan using the standard planner */
	elog( DEBUG3 , "planner_callback: standard planner");
	actual_plan = standard_planner( query, cursorOptions, boundParams );

	PG_TRY();
	{
	

		/* send the actual plan for comparison with a hypothetical plan */
		elog( DEBUG3 , "planner_callback: index_adviser");
		new_plan = index_adviser( queryCopy, cursorOptions, boundParams,
									actual_plan,NULL, false );
	}
	PG_CATCH();
	{
		elog(WARNING, "Failed to create index advice for: %s",debug_query_string);
		/* reset our 'running' state... */
		SuppressRecursion=0;
			
	}
	PG_END_TRY();

	/* TODO: try to free the redundant new_plan */
	elog( DEBUG3 , "planner_callback: Done");
	
	return actual_plan;
}

/*
 * This callback is registered immediately upon loading this plugin. It is
 * responsible for taking over control from the ExplainOneQuery() function.
 *
 *     It calls the standard planner and sends the resultant plan to
 * index_adviser() for comparison with a plan generated after creating
 * hypothetical indexes.
 *
 *     If the index_adviser() finds the hypothetical plan to be beneficial
 * than the real plan, it returns the hypothetical plan's copy so that this
 * hook can send it to the log.
 */
static void
ExplainOneQuery_callback(Query	*query,  IntoClause *into,
		      	ExplainState *stmt,
			const char	*queryString,
			ParamListInfo	params)
{
	Query		*queryCopy;
	PlannedStmt	*actual_plan;
	PlannedStmt	*new_plan;
	ListCell	*cell;
	instr_time planduration; // TODO: consider printing this as well

	resetSecondaryHooks();

	//elog(DEBUG1, "Original Query: ");
	/* planner() scribbles on it's input, so make a copy of the query-tree */
	queryCopy = copyObject( query );

	/* plan the query */
	actual_plan = standard_planner( query, 0, params );

	/* run it (if needed) and produce output */
	ExplainOnePlan( actual_plan, into, stmt, queryString, params
#if PG_VERSION_NUM >= 90400
                      , &planduration
#endif
                      );

	elog( DEBUG1 , "IND ADV: re-plan the query"); 

	PG_TRY();
	{
	
		/* re-plan the query */
		appendStringInfo(stmt->str, "\n** Plan with hypothetical indexes **\n");
		new_plan = index_adviser( queryCopy, 0, params, actual_plan,stmt, true );
		elog( DEBUG3 , "IND ADV: after call to Index_adviser");
		if ( new_plan )
		{
			bool analyze = stmt->analyze;

			stmt->analyze = false;
			elog( DEBUG1 , "got new plan");
		
			explain_get_index_name_hook = explain_get_index_name_callback;
			//DestReceiver *dest = CreateDestReceiver(DestDebug);
			//TupOutputState *tstate = begin_tup_output_tupdesc(dest, ExplainResultDesc(stmt));
			elog( INFO , "\n** Plan with Original indexes **\n");			
			//do_text_output_multiline(tstate, "\n** Plan with hypothetical indexes **\n"); /* separator line */
			//end_tup_output(tstate);
			ExplainOnePlan( new_plan, into, stmt, queryString, params
#if PG_VERSION_NUM >= 90400
                                      , &planduration
#endif
                                      );

			explain_get_index_name_hook = NULL;

			stmt->analyze = analyze;
		}
	}
	PG_CATCH();
	{
		elog(WARNING, "Failed to create index advice for: %s",debug_query_string);
		/* reset our 'running' state... */
		SuppressRecursion=0;
			
	}
	PG_END_TRY();

	/* The candidates might not have been destroyed by the Index Adviser, do it
	 * now. FIXME: this block belongs inside the 'if ( new_plan )' block above. */
	foreach( cell, index_candidates )
		pfree( (IndexCandidate*)lfirst( cell ) );

	list_free( index_candidates );

	index_candidates = NIL;

	foreach( cell, table_clauses )
        pfree( (RelClause*)lfirst( cell ) );

    list_free( table_clauses );
    table_clauses = NIL;

	/* TODO: try to free the now-redundant new_plan */
}

/*
 * get_relation_info() calls this callback after it has prepared a RelOptInfo
 * for a relation.
 *
 *     The Job of this callback is to fill in the information about the virtual
 * index, that get_rel_info() could not load from the catalogs. As of now, the
 * number of disk-pages that might be occupied by the virtual index (if created
 * on-disk), is the only information that needs to be updated.
 *
 * selectivity computations are basaed on "clause_selectivity" in src/backend/optimizer/path/clausesel.c:484
 *
 * Given the Oid of the relation, return the following info into fields
 * of the RelOptInfo struct:
 *  indexlist   list of IndexOptInfos for relation's indexes
 *  pages       number of pages
 *  tuples      number of tuples
 *
 * BUG: this fails if we have actual data in the table - must fix.
 */
static void get_relation_info_callback(	PlannerInfo	*root,
				Oid		relationObjectId,
				bool		inhparent,
				RelOptInfo	*rel)
{
	//ListCell *cell1;
	List       *indexoidlist;
    ListCell   *l;
    LOCKMODE    lmode;
	IndexCandidate *cand;
	Index       varno = rel->relid;
    Relation    relation;
    //bool        hasindex;
    List       *indexinfos = NIL;

	elog( DEBUG1, "IND ADV: get_relation_info_callback: ENTER." );
	relation = heap_open( relationObjectId, NoLock);
  
    indexoidlist = RelationGetIndexList(relation);

	elog( DEBUG3, "IND ADV: get_relation_info_callback: index list length %d",list_length(indexoidlist));
	lmode = AccessShareLock;
	foreach( l, indexoidlist)
	{
		Oid         indexoid = lfirst_oid(l);
		Relation    indexRelation;
		Form_pg_index index;
		IndexOptInfo *info;
		int         ncolumns;
		int         i;
		int			simpleColumns = 0;
		
		if( !is_virtual_index( indexoid, NULL ) ) { elog( DEBUG1, "IND ADV: get_relation_info_callback: real index - skipping "); continue; }// skip actual indexes
		
		elog( DEBUG1, "IND ADV: get_relation_info_callback: index list loop");
		indexRelation = index_open(indexoid, lmode);
  		index = indexRelation->rd_index;

		info = makeNode(IndexOptInfo);

		info->indexoid = index->indexrelid;
		info->reltablespace = RelationGetForm(indexRelation)->reltablespace;
		info->rel = rel;
		info->ncolumns = ncolumns = index->indnatts;		
		info->indexkeys = (int *) palloc(sizeof(int) * INDEX_MAX_KEYS);
		info->indexcollations = (Oid *) palloc(sizeof(Oid) * ncolumns);
		info->opfamily = (Oid *) palloc(sizeof(Oid) * ncolumns);
		info->opcintype = (Oid *) palloc(sizeof(Oid) * ncolumns);
                info->canreturn = (bool *) palloc(sizeof(bool) * ncolumns);
		elog( DEBUG3, "IND ADV: get_relation_info_callback: index oid: %d, ncols: %d",indexoid,ncolumns);
		
		for (i = 0; i < ncolumns; i++)
		{
                        elog( DEBUG3, "IDX_ADV: column %d  ",i);
			info->indexkeys[i] = index->indkey.values[i];
			if(info->indexkeys[i] != 0)
				simpleColumns +=1;
			info->indexcollations[i] = indexRelation->rd_indcollation[i]; //InvalidOid;			
			info->opfamily[i] = indexRelation->rd_opfamily[i];
			info->opcintype[i] = indexRelation->rd_opcintype[i];
#if PG_VERSION_NUM >= 90500
			info->canreturn[i] = index_can_return(indexRelation, i + 1);
#endif
		}
                elog( DEBUG3, "IDX_ADV: done with per column  ");
		for (; i < INDEX_MAX_KEYS; i++)
		{
			info->indexkeys[i] = 0;			
		} 

		//info->relam = indexRelation->rd_rel->relam;

		elog( DEBUG4 ,"IND ADV: amcostestimate=%d",info->amcostestimate);
		if(info->amcostestimate == InvalidOid)
		{
			elog( DEBUG4 ,"IND ADV: need to figure out the right valuse for amcostestimate");
			info->amcostestimate=(RegProcedure)1268; //btcostestimate
		}
#if PG_VERSION_NUM < 90500
		info->canreturn = index_can_return(indexRelation);
#endif
		info->amcanorderbyop = indexRelation->rd_am->amcanorderbyop;
		//info->amoptionalkey = indexRelation->rd_am->amoptionalkey;
		info->amsearcharray = indexRelation->rd_am->amsearcharray;
		//info->amsearchnulls = indexRelation->rd_am->amsearchnulls;
		info->amhasgettuple = OidIsValid(indexRelation->rd_am->amgettuple);
		info->amhasgetbitmap = OidIsValid(indexRelation->rd_am->amgetbitmap);
		
        info->amoptionalkey = false;
        info->amsearchnulls = false;
		
		/*
		* v9.4 introduced a concept of tree height for btree, we'll use unkonown for now
		* loot at _bt_getrootheight on how to estimate this.
		*/
#if PG_VERSION_NUM >= 90300
		info->tree_height = -1;
#endif
		/*
		* Fetch the ordering information for the index, if any.
		*/
                // TODO: how to handle non BTREE ops (support other index types, see: get_relation_info: plancat.c:88)		
		//if (info->relam == BTREE_AM_OID)
		if ( 1 == 1)
		{
			elog( DEBUG3 , "IND ADV: in BTREE_AM_OID");
			/*
			* If it's a btree index, we can use its opfamily OIDs
			* directly as the sort ordering opfamily OIDs.
			*/
			Assert(indexRelation->rd_am->amcanorder);

			info->sortopfamily = info->opfamily;
			info->reverse_sort = (bool *) palloc(sizeof(bool) * ncolumns);
			info->nulls_first = (bool *) palloc(sizeof(bool) * ncolumns);

			for (i = 0; i < ncolumns; i++)
			{
				int16       opt = indexRelation->rd_indoption[i];
			
				info->reverse_sort[i] = (opt & INDOPTION_DESC) != 0;
				info->nulls_first[i] = (opt & INDOPTION_NULLS_FIRST) != 0;
			}
		}			
		else
		{
			info->sortopfamily = NULL;
			info->reverse_sort = NULL;
			info->nulls_first = NULL;
		}
			
		elog( DEBUG3 , "IND ADV: almost there...");		
		/*
		* Fetch the index expressions and predicate, if any.  We must
		* modify the copies we obtain from the relcache to have the
		* correct varno for the parent relation, so that they match up
		* correctly against qual clauses.
		*/
		elog( DEBUG3 , "IND ADV: getting realtion expressions");		
		info->indexprs = RelationGetIndexExpressions(indexRelation);
		info->ncolumns = simpleColumns + list_length(info->indexprs); // TODO: why is this?!?! really ugly hack?!?

		elog( DEBUG3 , "IND ADV: get index predicates");		
		info->indpred = RelationGetIndexPredicate(indexRelation);
		elog( DEBUG3 , "IND ADV: change var nodes - expr");		
		if (info->indexprs && varno != 1)
		  ChangeVarNodes((Node *) info->indexprs, 1, varno, 0);
		elog( DEBUG3 , "IND ADV: change var nodes - pred");		
		if (info->indpred && varno != 1)
		  ChangeVarNodes((Node *) info->indpred, 1, varno, 0);

		//elog_node_display( DEBUG3 , "IND ADV: get_relation_info_callback: ", (const OpExpr*)list_nth(context->predicate,0),true);
		elog( DEBUG3 , "IND ADV: Build targetlist using the completed indexprs data");		

		/* Build targetlist using the completed indexprs data - used in index only scans */
		info->indextlist = build_index_tlist(root, info, relation);
		elog_node_display( DEBUG3, "IND ADV:  (fill in tlist )", info->indextlist, true );

		info->predOK = false;       /* set later in indxpath.c */
		info->unique = index->indisunique;
		info->immediate = index->indimmediate;
		info->hypothetical = true; // used to prevent access to the disc. see: src/backend/utils/adt/selfuncs.c -> get_actual_variable_range 

		/* We call estimate_index_pages() here, instead of immediately after
		 * index_create() API call, since rel has been run through
		 * estimate_rel_size() by the caller!
		 */
		elog( DEBUG1, "IND ADV: get_relation_info_callback: hypothetical? %s",BOOL_FMT(info->hypothetical));

		if( is_virtual_index( info->indexoid, &cand ) )
		{
			Selectivity btreeSelectivity;
			Node       *left,  *right;
			Var        *var;
			Const		*cons;
			VariableStatData ldata,rdata;
			VariableStatData *vardata;
						
			elog( DEBUG3 , "IND ADV: get index predicates args");
			elog_node_display( DEBUG3, "IND ADV:  (info->indpred)", info->indpred, true );
			if (info->indpred){ // TODO: do i need the varno != 1
				OpExpr     *opclause = (OpExpr *) linitial(info->indpred);			
				Oid         opno = opclause->opno;
				RegProcedure oprrest = get_oprrest(opno);
				elog( DEBUG3 , "IND ADV: get opno 2 %d",opno);
				elog( DEBUG3 , "IND ADV: get oprrest 2 %d",oprrest);

				/* TODO: add support for boolean selectivity, create a " var = 't' " clause */
				if(not_clause(opclause))
				{					
					elog( DEBUG3 , "IND ADV: boolean not expression - todo: compute selectivity");
					var = (Var *) get_notclausearg((Expr *) opclause);
					cons = (Const *) makeBoolConst(false,false);	
					opno = BooleanNotEqualOperator   ;
				}
				else if(IsA(opclause, Var))
				{
					elog( DEBUG3 , "IND ADV: var expression - todo: compute selectivity");
					var = (Var *) opclause;
					cons = (Const *) makeBoolConst(true,false);
					opno = BooleanEqualOperator;
				}
				else
				{

					left = (Node *) linitial(opclause->args);
					right = (Node *) lsecond(opclause->args);
					/*
					 * Examine both sides.  Note that when varRelid is nonzero, Vars of other
					 * relations will be treated as pseudoconstants.
					 */
					elog( DEBUG3 , "IND ADV: get oprrest3");
					examine_variable(root, left,  cand->idxoid, &ldata);
					examine_variable(root, right, cand->idxoid, &rdata);
			
					/* Set up result fields other than the stats tuple */
					if(IsA(right, Var))
					{
						var = (Var *) right;
						cons = (Const *) left;
					}else
					{
						var = (Var *) left;
						cons = (Const *) right;
					}
				}
				elog( DEBUG3 , "IND ADV: get oprrest 4");

				elog( DEBUG4, "IND ADV: get_relation_info_callback:  pallocate mem for vardata, size: %ld",sizeof(VariableStatData));
				vardata = palloc(sizeof(VariableStatData));

				vardata->var = (Node *)var;    /* return Var without relabeling */
				vardata->rel = rel;
				vardata->atttype = var->vartype;
				vardata->atttypmod = var->vartypmod;
				vardata->isunique = has_unique_index(vardata->rel, var->varattno);
				/* Try to locate some stats */			
				vardata->statsTuple = SearchSysCache3(STATRELATTINH,
													ObjectIdGetDatum(relationObjectId),
													Int16GetDatum(var->varattno),
													BoolGetDatum(false));
				vardata->freefunc = ReleaseSysCache;
				elog( DEBUG3, "IND ADV: get_relation_info_callback: %s stats found for %d",(vardata->statsTuple == NULL) ? "No":"",relationObjectId);
			

				elog( DEBUG3, "IND ADV: get_relation_info_callback: estimate virtual index pages for: %d",cand->idxoid);
				elog( DEBUG3, "IND ADV: get_relation_info_callback: opno: %d",opno);
				elog( DEBUG3, "IND ADV: get_relation_info_callback: cluse type : %d",nodeTag((Node *) linitial(info->indpred)));
				elog( DEBUG3, "IND ADV: get_relation_info_callback: oprrest : %d",oprrest);
				//elog( DEBUG3, "IND ADV: get_relation_info_callback: arg list length : %d",list_length(opclause->args));

				/* Estimate selectivity for a restriction clause. */            
				btreeSelectivity = var_eq_cons(vardata, opno,cons->constvalue,
								cons->constisnull,true);		
				
			}else
			{
				elog( DEBUG3, "IND ADV: get_relation_info_callback: no index predicates");
				btreeSelectivity = 1;
			}

			elog( DEBUG3, "IND ADV: get_relation_info_callback: selectivity = %.5f", btreeSelectivity);
			
			/* estimate the size */
			cand->pages = (BlockNumber)lrint(btreeSelectivity * estimate_index_pages(cand->reloid, cand->idxoid));
			if(cand->pages == 0) // we must allocate at least 1 page
				cand->pages=1;
			info->pages = cand->pages;
			elog( DEBUG3, "IDX_ADV: get_relation_info_callback: pages: %d",info->pages);
			info->tuples = (int) ceil(btreeSelectivity * rel->tuples);
			cand->tuples = (int) ceil(btreeSelectivity * rel->tuples);
			//elog( DEBUG3, "IND ADV: get_relation_info_callback: tuples: %d",info->tuples);
		}
		index_close(indexRelation, NoLock);
		elog( DEBUG3 , "add the index to the indexinfos list");
		indexinfos = lcons(info, indexinfos);
	}
        heap_close(relation, NoLock);
	rel->indexlist = indexinfos;
	elog( DEBUG1, "IDX ADV: get_relation_info_callback: cand list length %d",list_length(rel->indexlist));

    elog( DEBUG1, "IDX ADV: get_relation_info_callback: EXIT");
}

/* Use this function to reset the hooks that are required to be registered only
 * for a short while; these may have been left registered by the previous call, in
 * case of an ERROR.
 */
static void resetSecondaryHooks()
{
	get_relation_info_hook		= NULL;
	explain_get_index_name_hook	= NULL;
}

static bool is_virtual_index( Oid oid, IndexCandidate **cand_out )
{
	ListCell *cell1;

	foreach( cell1, index_candidates )
	{
		IndexCandidate *cand = (IndexCandidate*)lfirst( cell1 );
		//elog( DEBUG4 ,"is_virtual_index compare: oid:%d, cand:%d",oid,cand->idxoid);
		if( cand->idxoid == oid )
		{
			if( cand_out )
				*cand_out = cand;
			return true;
		}
	}

	return false;
}

static const char * explain_get_index_name_callback(Oid indexId)
{
	StringInfoData buf;
	IndexCandidate *cand;
	
	elog( DEBUG1 ,"explain_get_index_name_callback: ENTER - looking at oid: %d",indexId);

	if( is_virtual_index( indexId, &cand ) )
	{
		elog( DEBUG1 ,"explain_get_index_name_callback: our virt index");
		initStringInfo(&buf);

		appendStringInfo( &buf, "<V-Index>:%d", cand->idxoid );

		return buf.data;
	}

	elog( DEBUG1 ,"explain_get_index_name_callback: EXIT - not ours");
	return NULL;                            /* allow default behavior */
}


 /*!
  * store_idx_advice
  * \brief for every candidate insert an entry into IDX_ADV_TABL.
  * @param  List* of candidates
  */
static void store_idx_advice( List* candidates , ExplainState * 	es )
{
	StringInfoData	query;	/*!< string for Query */	
	StringInfoData	cols;	/*!< string for Columns */
	StringInfoData	pcols;	/*!< string for Partial clause Columns  */
	StringInfoData	pvals;	/*!< string for Partial clause Values */
	StringInfoData	op_class;	/*!< string for op class family */
	StringInfoData	collationObjectId;	/*!< string for collation object */
	StringInfoData	attList;	/*!< string for functional attributes */
	StringInfoData	partialClause;	/*!< string for partial clause */
	StringInfoData	indexDef;	/*!< string for index definition */
	Oid				advise_oid;
	List       *context;
	ListCell		*cell;
	ListCell   *indexpr_item;
	List *rel_clauses = NIL;
	

	elog( DEBUG2, "IDX_ADV: store_idx_advice: ENTER" );

	Assert( list_length(candidates) != 0 );

	/*
	 * Minimal check: check that IDX_ADV_TABL is at least visible to us. There
	 * are a lot more checks we should do in order to not let the INSERT fail,
	 * like permissions, datatype mis-match, etc., but we leave those checks
	 * upto the executor.
	 */

	/* find a relation named IDX_ADV_TABL on the search path */
	advise_oid = RelnameGetRelid( IDX_ADV_TABL );

	if (advise_oid != InvalidOid)
	{
		Relation advise_rel = relation_open(advise_oid, AccessShareLock);

		if (advise_rel->rd_rel->relkind != RELKIND_RELATION
			&& advise_rel->rd_rel->relkind != RELKIND_VIEW)
		{
			relation_close(advise_rel, AccessShareLock);

			/* FIXME: add errdetail() and/or errcontext() calls here. */
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg( IDX_ADV_ERROR_NTV )));
		}

		relation_close(advise_rel, AccessShareLock);
	}
	else
	{
		/* FIXME: add errdetail() and/or errcontext() calls here. */
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg( IDX_ADV_ERROR_NE )));
	}

	initStringInfo( &query );	
	initStringInfo( &cols );
	initStringInfo( &pvals );
	initStringInfo( &pcols );
	initStringInfo( &op_class );
	initStringInfo( &collationObjectId );
	initStringInfo( &attList );
	initStringInfo( &partialClause );
	initStringInfo( &indexDef );
	
	foreach( cell, candidates )
	{
		int i;
		//int val;
		IndexCandidate* idxcd = (IndexCandidate*)lfirst( cell );

		if( !idxcd->idxused )
			continue;

		/* pfree() the memory allocated for the previous candidate. FIXME: Avoid
		 * meddling with the internals of a StringInfo, and try to use an API.
		 */
		/*if( cols.len > 0 )
		{
			pfree( cols.data );
			cols.data = NULL;
		}*/
		
		resetStringInfo( &query );	
		resetStringInfo( &cols );
		resetStringInfo( &pvals );
		resetStringInfo( &pcols );
		resetStringInfo( &op_class );
		resetStringInfo( &collationObjectId );
		resetStringInfo( &attList );
		resetStringInfo( &partialClause );
		resetStringInfo( &indexDef );

		//initStringInfo( &cols );
		indexpr_item = list_head(idxcd->attList);
		context = deparse_context_for(idxcd->erefAlias, idxcd->reloid);

		for (i = 0; i < idxcd->ncols; ++i){
			Oid         keycoltype;

			appendStringInfo( &cols, "%s%d", (i>0?",":""), idxcd->varattno[i]);
			appendStringInfo( &op_class, "%s%d", (i>0?",":""), idxcd->op_class[i]);
			appendStringInfo( &collationObjectId, "%s%d", (i>0?",":""), idxcd->collationObjectId[i]);			

			if (idxcd->varattno[i] == 0)
			{
				/* expressional index */
				Node       *indexkey;
							
				indexkey = (Node *) lfirst(indexpr_item);
				indexpr_item = lnext(indexpr_item);	
				keycoltype = exprType(indexkey); // get the attribut column type
				//elog( DEBUG2 , "IND ADV: store_idx_advice: column collation: %d",exprCollation(indexkey)); // get the attribut collation
				appendStringInfo(&attList,"%s%s", (i>0?",":""),deparse_expression(indexkey, context, false, false));
				get_opclass_name(idxcd->op_class[i], keycoltype, &attList);
				//elog( DEBUG2 , "IND ADV: store_idx_advice: column: %s",deparse_expression(indexkey, context, false, false));
				//elog( DEBUG2 , "IND ADV: store_idx_advice: column opclass: %s",attList.data);				
			}
			else
			{
				//elog( DEBUG2 , "IND ADV: store_idx_advice: column name: %s",get_attname(idxcd->reloid,idxcd->varattno[i]));
				appendStringInfo(&attList,"%s%s", (i>0?",":""),get_attname(idxcd->reloid,idxcd->varattno[i]));
			}
		}
		//elog( DEBUG2 , "IND ADV: store_idx_advice: const exsits %s",pg_get_indexdef_columns(idxcd->idxoid,2));
		
		//appendStringInfo(&attList,TextDatumGetCString(DirectFunctionCall2(pg_get_expr,CStringGetTextDatum(nodeToString(make_ands_explicit(idxcd->attList))), ObjectIdGetDatum(idxcd->reloid))));
		//elog( DEBUG2 , "IND ADV: store_idx_advice - idxcd->attList: %s",TextDatumGetCString(DirectFunctionCall2(pg_get_expr,CStringGetTextDatum(nodeToString(make_ands_explicit(idxcd->attList))), ObjectIdGetDatum(idxcd->reloid))));		
		// TODO: go over this in a loop
		if(table_clauses != NIL){
			//int j=0;
			//ListCell		*clausCell;
			rel_clauses = get_rel_clauses(table_clauses, idxcd->reloid,idxcd->erefAlias);	
			//elog( INFO , "IND ADV: store_idx_advice: no where clause oid: %d, alias: %s, name: %s",idxcd->reloid,idxcd->erefAlias,get_rel_name(idxcd->reloid));		
			
			//elog( DEBUG2 , "IND ADV: store_idx_advice - rel_clauses: %s",TextDatumGetCString(DirectFunctionCall2(pg_get_expr,CStringGetTextDatum(nodeToString(make_ands_explicit(rel_clauses))), ObjectIdGetDatum(idxcd->reloid))));		
			if(rel_clauses != NIL){
				 appendStringInfoString(&partialClause,deparse_expression((Node *)make_ands_explicit(rel_clauses), context, false, false));		
			}
			//elog( DEBUG2 , "IND ADV: store_idx_advice: rel_clauses: %s",deparse_expression(make_ands_explicit(rel_clauses), context, false, false));
		} else
		{
			elog( DEBUG3 , "IND ADV: store_idx_advice: no where clause");			
		}

		appendStringInfo( &indexDef,"create index on %s(%s)%s%s",get_rel_name(idxcd->reloid),attList.data,partialClause.len>0?" where":"",partialClause.len>0?partialClause.data:"");
		
		/* FIXME: Mention the column names explicitly after the table name. */
		appendStringInfo( &query, "insert into %s.\""IDX_ADV_TABL"\" values ( %d, array[%s], %f, %d, %d, now(),array[%s],array[%s],array[%s],$$%s$$,$$%s$$,$$%s$$,$$%s$$);",
									idxadv_schema,
									idxcd->reloid,
									cols.data,
									idxcd->benefit,
									idxcd->pages * BLCKSZ/1024, /* in KBs */
									MyProcPid,																		
									collationObjectId.data,
									op_class.data,
									op_class.data,
									nodeToString(idxcd->attList),
									nodeToString(rel_clauses),									
									strstr(debug_query_string,"explain ")!=NULL?(debug_query_string+8):debug_query_string, /* the explain cmd without the "explain " at the begining... - if it's not found return the original string*/									
									indexDef.data);		
		//elog( DEBUG4 , "IND ADV: store_idx_advice: build the index: create index on %s(%s)%s%s",idxcd->erefAlias,attList.data,partialClause.len>0?" where":"",partialClause.len>0?partialClause.data:"");

		if( query.len > 0 )	/* if we generated any SQL */
		{
			//appendStringInfo(es->str, "read only, advice, index: %s\n",indexDef.data);
			
			elog(DEBUG1, "IDX ADV: read only, advice: %s, \n index: %s\n",query.data,indexDef.data);
			if (es != NULL) { appendStringInfo(es->str, "read only, advice, index: %s\n",indexDef.data); }
			
			elog( DEBUG1, "SPI connection start - save advice");
			if( SPI_connect() == SPI_OK_CONNECT )
			{
				elog( DEBUG1 , "IND ADV: store_idx_advice: build the insert query %s",query.data);
				if( SPI_execute( query.data, false, 0 ) != SPI_OK_INSERT )
						elog( WARNING, "IND ADV: SPI_execute failed while saving advice." );			
				
				elog( DEBUG1, "SPI connection finish");
				if( SPI_finish() != SPI_OK_FINISH )
					elog( WARNING, "IND ADV: SPI_finish failed while saving advice." );
			}
			else
				elog( WARNING, "IND ADV: SPI_connect failed while saving advice." );
		}
	} 

	
	/* TODO: Propose to -hackers to introduce API to free a StringInfoData . */
	if ( query.len > 0 )
		pfree( query.data );
	
	if ( cols.len > 0 )
		pfree( cols.data );

	if ( pcols.len > 0 )
		pfree( pcols.data );

	if ( pvals.len > 0 )
		pfree( pvals.data );

	if ( attList.len > 0 )
		pfree( attList.data );

	if ( partialClause.len > 0 )
		pfree( partialClause.data );
	if ( indexDef.len > 0 )
		pfree( indexDef.data );
	

	elog( DEBUG3, "IND ADV: store_idx_advice: EXIT" );
}

/**
 * remove_irrelevant_candidates
 *
 * A candidate is irrelevant if it has one of the followingg properties:
 *
 * (a) it indexes an unsupported relation (system-relations or temp-relations)
 * (b) it matches an already present index.
 *
 * TODO Log the candidates as they are pruned, and remove the call to
 * log_candidates() in index_adviser() after this function is called.
 *
 * REALLY BIG TODO/FIXME: simplify this function.
 *
 */
static List* remove_irrelevant_candidates( List* candidates )
{
	ListCell *cell = list_head(candidates);
	ListCell *prev = NULL;

	while(cell != NULL)
	{
		ListCell *old_cell = cell;

		Oid base_rel_oid = ((IndexCandidate*)lfirst( cell ))->reloid;		
		Relation base_rel = heap_open( base_rel_oid, AccessShareLock );

		/* decide if the relation is unsupported. This check is now done before
		 * creating a candidate in scan_generic_node(); but still keeping the
		 * code here.
		 */
		// if((base_rel->rd_istemp == true) replaced 
                if((!RelationNeedsWAL(base_rel))
			|| IsSystemRelation(base_rel))
		{
			ListCell *cell2;
			ListCell *prev2;
			ListCell *next;

			/* remove all candidates that are on currently unsupported relations */
			elog( DEBUG1,
					"Index candidate(s) on an unsupported relation (%d) found!",
					base_rel_oid );

			/* Remove all candidates with same unsupported relation */
			for(cell2 = cell, prev2 = prev; cell2 != NULL; cell2 = next)
			{
				next = lnext(cell2);

				if(((IndexCandidate*)lfirst(cell2))->reloid == base_rel_oid)
				{
					pfree((IndexCandidate*)lfirst(cell2));
					candidates = list_delete_cell( candidates, cell2, prev2 );

					if(cell2 == cell)
						cell = next;
				}
				else
				{
					prev2 = cell2;
				}
			}
		}
		else
		{
			/* Remove candidates that match any of already existing indexes.
			 * The prefix old_ in these variables means 'existing' index
			*/

			/* get all index Oids */
			ListCell	*index_cell;
			List		*old_index_oids = RelationGetIndexList( base_rel );

			foreach( index_cell, old_index_oids )
			{
				/* open index relation and get the index info */
				Oid			old_index_oid	= lfirst_oid( index_cell );
				Relation	old_index_rel	= index_open( old_index_oid,
										AccessShareLock );
				IndexInfo	*old_index_info	= BuildIndexInfo( old_index_rel );

				/* We ignore expressional indexes and partial indexes */
				if( old_index_rel->rd_index->indisvalid
					&& old_index_info->ii_Expressions == NIL
					&& old_index_info->ii_Predicate == NIL )
				{
					ListCell *cell2;
					ListCell *prev2;
					ListCell *next;

					Assert( old_index_info->ii_Expressions == NIL );
					Assert( old_index_info->ii_Predicate == NIL );

					/* search for a matching candidate */
					for(cell2 = cell, prev2 = prev;
						cell2 != NULL;
						cell2 = next)
					{next = lnext(cell2);{ /* FIXME: move this line to the block below; it doesn't need to be here. */

						IndexCandidate* cand = (IndexCandidate*)lfirst(cell2);

						signed int cmp = (signed int)cand->ncols
											- old_index_info->ii_NumIndexAttrs;

						if(cmp == 0)
						{
							int i = 0;
							do
							{
								cmp =
									cand->varattno[i]
									- old_index_info->ii_KeyAttrNumbers[i];
								++i;
							/* FIXME: should this while condition be: cmp==0&&(i<min(ncols,ii_NumIndexAttrs))
 							 * maybe this is to eliminate candidates that are a prefix match of an existing index. */
							} while((cmp == 0) && (i < cand->ncols));
						}

						if(cmp != 0)
						{
							/* current candidate does not match the current
							 * index, so go to next candidate.
							 */
							prev2 = cell2;
						}
						else
						{
							elog( DEBUG1,
									"A candidate matches the index oid of : %d;"
										"hence ignoring it.",
							 		old_index_oid );

							/* remove the candidate from the list */
							candidates = list_delete_cell(candidates,
															cell2, prev2);
							pfree( cand );

							/* If we just deleted the current node of the outer-most loop, fix that. */
							if (cell2 == cell)
								cell = next;

							break;	/* while */
						}
					}} /* for */
				}

				/* close index relation and free index info */
				index_close( old_index_rel, AccessShareLock );
				pfree( old_index_info );
			}

			/* free the list of existing index Oids */
			list_free( old_index_oids );

			/* clear the index-list, else the planner can not see the
			 * virtual-indexes
			 * TODO: Really?? Verify this.
			 */
			base_rel->rd_indexlist  = NIL;
			base_rel->rd_indexvalid = 0;
		}

		/* close the relation */
		heap_close( base_rel, AccessShareLock );

		/*
		 * Move the pointer forward, only if the crazy logic above did not do it
		 * else, cell is already pointing to a new list-element that needs
		 * processing
		 */
		if(cell == old_cell)
		{
			prev = cell;
			cell = lnext(cell);
		}
	}

	return candidates;
}

/**
 * tag_and_remove_candidates
 *    tag every candidate we do use and remove those unneeded
 * Note: should i really remove or wait for cleanup?.
 */
static void tag_and_remove_candidates(Cost startupCostSaved, Cost totalCostSaved,PlannedStmt		*new_plan, const Node* const head,List* const candidates)
{
	
	if( startupCostSaved >0 || totalCostSaved > 0 )
	{
		/* scan the plan for virtual indexes used */
		plannedStmtGlobal = new_plan;

		//elog_node_display( DEBUG4, "plan (using Index Adviser)",(Node*)new_plan->planTree, true );

		mark_used_candidates( head, candidates );

		plannedStmtGlobal = NULL;
	}
	
	elog( DEBUG3, "IND ADV: Remove unused candidates from the list" );
	/* Remove unused candidates from the list. */
	/*for( prev = NULL, cell = list_head(candidates);
			cell != NULL;
			cell = next )
	{
		IndexCandidate *cand = (IndexCandidate*)lfirst( cell );

		next = lnext( cell );

		if( !cand->idxused )
		{
			pfree( cand );
			candidates = list_delete_cell( candidates, cell, prev );
		}
		else
			prev = cell;
	}*/
}

/**
 * mark_used_candidates
 *    scan the execution plan to find hypothetical indexes used by the planner
 * Note: there is no tree_walker for execution plans so we need to drill down ourselves.
 */
static void mark_used_candidates(const Node* const node, List* const candidates)
{
	const ListCell	*cell;
	bool			planNode = true;	/* assume it to be a plan node */

	elog( DEBUG3, "IND ADV: mark_used_candidates: ENTER" );	

     //TODO: remove this
	//foreach( cell, candidates )
	//{
	//	/* is virtual-index-oid in the IndexScan-list? */
 //   	IndexCandidate* const idxcd = (IndexCandidate*)lfirst( cell );
 //   	idxcd->idxused = true;
	//}
	//return;
	elog( DEBUG3, "IND ADV: mark_used_candidates: node tag: %d ", nodeTag( node ) );
	switch( nodeTag( node ) )
	{
		/* if the node is an indexscan */
		case T_IndexScan: // TAG: 110
		{
			/* are there any used virtual-indexes? */
			const IndexScan* const idxScan = (const IndexScan*)node;
			elog( DEBUG3, "IND ADV: mark_used_candidates: plan idx: %d ", idxScan->indexid );

			foreach( cell, candidates )
			{

				/* is virtual-index-oid in the IndexScan-list? */
				IndexCandidate* const idxcd = (IndexCandidate*)lfirst( cell );
				elog( DEBUG3, "IND ADV: mark_used_candidates: cand idx: %d ", idxcd->idxoid );
				const bool used = (idxcd->idxoid == idxScan->indexid);

				/* connect the existing value per OR */
				idxcd->idxused = (idxcd->idxused || used);

			}
		}
		break;		
		case T_IndexOnlyScan: // TAG: 110
		{
			/* are there any used virtual-indexes? */
			const IndexOnlyScan* const idxScan = (const IndexOnlyScan*)node;
			elog( DEBUG3, "IND ADV: mark_used_candidates: plan idx: %d ", idxScan->indexid );

			foreach( cell, candidates )
			{

				/* is virtual-index-oid in the IndexScan-list? */
				IndexCandidate* const idxcd = (IndexCandidate*)lfirst( cell );
				elog( DEBUG3, "IND ADV: mark_used_candidates: cand idx: %d ", idxcd->idxoid );
				const bool used = (idxcd->idxoid == idxScan->indexid);

				/* connect the existing value per OR */
				idxcd->idxused = (idxcd->idxused || used);

			}
		}
		break;


		/* if the node is a bitmap-index-scan */
		case T_BitmapIndexScan:
		{
			/* are there any used virtual-indexes? */
			const BitmapIndexScan* const bmiScan = (const BitmapIndexScan*)node;
			elog( DEBUG3, "IND ADV: mark_used_candidates: plan idx: %d ", bmiScan->indexid );

			foreach( cell, candidates )
			{
				/* is virtual-index-oid in the BMIndexScan-list? */
				IndexCandidate* const idxcd = (IndexCandidate*)lfirst( cell );
				elog( DEBUG3, "IND ADV: mark_used_candidates: cand idx: %d ", idxcd->idxoid );
				const bool used = idxcd->idxoid == bmiScan->indexid;

				/* conntect the existing value per OR */
				idxcd->idxused = idxcd->idxused || used;
			}
		}
		break;

		/* if the node is a bitmap-and */
		case T_BitmapAnd:
		{
			/* are there any used virtual-indexes? */
			const BitmapAnd* const bmiAndScan = (const BitmapAnd*)node;

			foreach( cell, bmiAndScan->bitmapplans )
				mark_used_candidates( (Node*)lfirst( cell ), candidates );
		}
		break;

		/* if the node is a bitmap-or */
		case T_BitmapOr:
		{
			/* are there any used virtual-indexes? */
			const BitmapOr* const bmiOrScan = (const BitmapOr*)node;

			foreach( cell, bmiOrScan->bitmapplans )
				mark_used_candidates( (Node*)lfirst( cell ), candidates );
		}
		break;

		case T_SubqueryScan:
		{
			/* scan subqueryplan */
			const SubqueryScan* const subScan = (const SubqueryScan*)node;
			mark_used_candidates( (const Node*)subScan->subplan, candidates );
		}
		break;
		
		case T_NestLoop:
		case T_MergeJoin:
		case T_HashJoin:
		case T_Join:
		{
			/* scan join-quals */
			const Join* const join = (const Join*)node;

			foreach( cell, join->joinqual )
			{
				const Node* const qualPlan = (const Node*)lfirst( cell );
				mark_used_candidates( qualPlan, candidates );
			}
		}
		break;

		case T_OpExpr:
		{
			const OpExpr* const expr = (const OpExpr*)node;

			planNode = false;

			foreach( cell, expr->args )
				mark_used_candidates( (const Node*)lfirst( cell ), candidates );
		}
		break;

		case T_SubPlan:
		{
			/* scan the subplan */
			const SubPlan* const subPlan = (const SubPlan*)node;

			planNode = false;

			mark_used_candidates( (const Node*)&plannedStmtGlobal->subplans[subPlan->plan_id], candidates );
		}
		break;


		case T_BoolExpr:
		{
			const BoolExpr* const expr = (const BoolExpr*)node;

			planNode = false;

			foreach( cell, expr->args )
			{
				const Node* const nodeBool = (const Node*)lfirst( cell );
				mark_used_candidates( nodeBool, candidates );
			}
		}
		break;
		/*
		case T_CoerceViaIO:
		{
			elog_node_display( DEBUG3 , "T_CoerceViaIO", node,true);
		}
		break;
		*/

		case T_FunctionScan:
		case T_CteScan:
		case T_RecursiveUnion:
		case T_Result:
		case T_Append:
		case T_TidScan:
		case T_Material:
		case T_Sort:
		case T_Group:
		case T_Agg:
		case T_WindowAgg:
		case T_Unique:
		case T_Hash:
		case T_SetOp:
		case T_Limit:
		case T_Scan:
		case T_SeqScan:
		case T_BitmapHeapScan:	
		
		break;

		case T_AlternativeSubPlan:
		case T_FuncExpr:
		case T_Const:
		case T_CoerceViaIO:
		case T_ArrayCoerceExpr:
		case T_Var:
			planNode = false;
		break;

		/* report parse-node types that we missed */
		default:
		{
			elog( NOTICE, "IND ADV: unhandled plan-node type: %d; Query: %s\n",
					(int)nodeTag( node ), debug_query_string );
			planNode = false;	/* stop scanning the tree here */
		}
		break;
	}

	if( planNode )
	{
		const Plan* const plan = (Plan *) node;

		if( plan->initPlan )
		{
			ListCell *cell;

			foreach( cell, ((Plan*)node)->initPlan )
			{
				SubPlan *subPlan = (SubPlan*)lfirst( cell );

				mark_used_candidates( (Node*)exec_subplan_get_plan(
															plannedStmtGlobal,
															subPlan),
										candidates );
			}
		}

		if( IsA(((Node*)plan), Append) )
		{
			Append	*appendplan = (Append *)node;
			ListCell *cell;

			foreach( cell, appendplan->appendplans )
			{
				Plan *child = (Plan*)lfirst( cell );

				mark_used_candidates( (Node*)child, candidates );
			}
		}


		/* scan left- and right-tree */
		if( outerPlan(plan) )
			mark_used_candidates( (const Node*)outerPlan(plan), candidates );

		if( innerPlan(plan) )
			mark_used_candidates( (const Node*)innerPlan(plan), candidates );

		/* walk through the qual-list */
		foreach( cell, plan->qual )
		{
			const Node* const nodeQual = (const Node*)lfirst( cell );
			mark_used_candidates( nodeQual, candidates );
		}
	}

	elog( DEBUG3, "IND ADV: mark_used_candidates: EXIT" );
}

/**
 * scan_query
 *    Runs thru the whole query to find columns to create index candidates.
 *   Note: We do not use the query_tree_walker here because it doesn't go into 
 * GROUP BY, ORDER BY, and we also add aditional handeling of inheritence table expension.
 */
static List* scan_query(	const Query* const query,
					List* const opnos,
					List* rangeTableStack )
{
	const ListCell*	cell;
	List*		candidates		= NIL;
	List*		newCandidates	= NIL;

	elog( DEBUG4, "IND ADV: scan_query: ENTER" );	

	/* add the current rangetable to the stack */
	rangeTableStack = lcons( query->rtable, rangeTableStack );
	// TODO: can i use this to allow inheritance?
	//elog_node_display( DEBUG4 , "query - print whole", query,true);

	/* scan CTE-queries */
	foreach( cell, query->cteList )
	{
		const CommonTableExpr* const rte = (const CommonTableExpr*)lfirst( cell );
		elog( DEBUG3 , "IND ADV: scan_query: CTE working on: %s",rte->ctename);		
		
		if( rte->ctequery )
		{
			elog_node_display( DEBUG4 , "CTE query", rte->ctequery,true);
			candidates = merge_candidates( candidates, scan_query(
										rte->ctequery,
										opnos,
										rangeTableStack));
		}
	}
	
	/* scan sub-queries */
	foreach( cell, query->rtable )
	{
		const RangeTblEntry* const rte = (const RangeTblEntry*)lfirst( cell );
		elog( DEBUG3 , "IND ADV: scan_query: SUB working on: %s",rte->eref->aliasname);		
		//elog( DEBUG3 , "IND ADV: scan_query: working on: %d",rte->rtekind);		
		
		if( rte->subquery )
		{
			elog_node_display( DEBUG4 , "sub query", rte->subquery,true);
			candidates = merge_candidates( candidates, scan_query(
										rte->subquery,
										opnos,
										rangeTableStack));
		}
		
		/* adding support for join clause */
		if (rte->joinaliasvars)
		{
			//elog( DEBUG3 , "IND ADV: scan_query: join");	
			//elog( DEBUG3 , "IND ADV: scan_query: join type : %d",rte->jointype);					
			//elog_node_display( DEBUG4 , "sub query", rte->joinaliasvars,true);
			candidates = merge_candidates( candidates, scan_generic_node( rte->joinaliasvars,
							opnos,
							rangeTableStack));
		}
	}

	/* scan "where" from the current query */
	if( query->jointree->quals != NULL )
	{
		newCandidates = scan_generic_node(	query->jointree->quals, opnos,
							rangeTableStack );		
	}

	/* FIXME: Why don't we consider the GROUP BY and ORDER BY clause
	 * irrespective of whether we found candidates in WHERE clause?
	 */
        elog( DEBUG3, "IND ADV: scan_query: at FIXME");

	/* if no indexcadidate found in "where", scan "group" */
	if( ( newCandidates == NIL ) && ( query->groupClause != NULL ) )
	{
		newCandidates = scan_group_clause(	query->groupClause,
							query->targetList,
							opnos,
							rangeTableStack );
	}

	/* if no indexcadidate found in "group", scan "order by" */
	if( ( newCandidates == NIL ) && ( query->sortClause != NULL ) )
	{
		newCandidates = scan_group_clause(	query->sortClause,
							query->targetList,
							opnos,
							rangeTableStack );
	}
	/* if no indexcadidate found until now, scan the target list "select" */
	if( ( newCandidates == NIL ) && ( query->targetList != NULL ) )
	{
		newCandidates = scan_targetList(	query->targetList,
							opnos,
							rangeTableStack );
	}

	/* remove the current rangetable from the stack */
	rangeTableStack = list_delete_ptr( rangeTableStack, query->rtable );

	/* merge indexcandiates */
	candidates = merge_candidates( candidates, newCandidates );

	/* expend inherited tables */
	candidates = expand_inherited_candidates( candidates);
	expand_inherited_rel_clauses();

	elog( DEBUG3, "IND ADV: scan_query: EXIT" );

	return candidates;
}

/**
 * scan_group_clause
 *    Runs thru the GROUP BY clause looking for columns to create index candidates.
 */
static List* scan_group_clause(	List* const groupList,
			List* const targetList,
			List* const opnos,
			List* const rangeTableStack )
{
	const ListCell*	cell;
	List* candidates = NIL;

	elog( DEBUG3, "IND ADV: scan_group_clause: ENTER" );

	/* scan every entry in the group-list */
	foreach( cell , groupList )
	{
		/* convert to group-element */
		const SortGroupClause* const groupElm = (const SortGroupClause*)lfirst( cell );

		/* get the column the group-clause is for */
		const TargetEntry* const targetElm = list_nth( targetList,
								groupElm->tleSortGroupRef - 1);

		/* scan the node and get candidates */
		const Node* const node = (const Node*)targetElm->expr;

		candidates = merge_candidates( candidates, scan_generic_node( node,
							opnos,
							rangeTableStack));
	}

	elog( DEBUG3, "IND ADV: scan_group_clause: EXIT" );

	return candidates;
}


/**
 * scan_targetList
 *    Runs thru the GROUP BY clause looking for columns to create index candidates.
 */
static List* scan_targetList(	List* const targetList,
				List* const opnos,
				List* const rangeTableStack )
{
	const ListCell*	cell;
	List* candidates = NIL;

	elog( DEBUG3, "IND ADV: scan_targetList: ENTER" );

	/* scan every entry in the target-list */
	foreach( cell , targetList )
	{
		/* convert to TargetEntry - the column with the expression */
		const TargetEntry* const targetElm = (const TargetEntry*)lfirst( cell );

		/* scan the node and get candidates */
		const Node* const node = (const Node*)targetElm->expr;

		candidates = merge_candidates(  candidates, scan_generic_node( node,
						opnos,
						rangeTableStack));
	}

	elog( DEBUG3, "IND ADV: scan_targetList: EXIT" );

	return candidates;
}


static bool index_candidates_walker (Node *root, ScanContext *context)
{
	ListCell*		cell;
	List*			candidates = NIL;

	elog( DEBUG4, "IND ADV: scan_generic_node: ENTER" );

	   if (root == NULL)
       return false;
   	// check for nodes that special work is required for, eg:

	
	elog( DEBUG4, "IND ADV: scan_generic_node, tag: %d", nodeTag( root ));
	switch( nodeTag( root ) )
	{

		/* if the node is a boolean-expression */
		case T_BoolExpr:
		{
			const BoolExpr* const expr = (const BoolExpr*)root;

			if( expr->boolop != AND_EXPR )
			{
				/* non-AND expression */
				Assert( expr->boolop == OR_EXPR || expr->boolop == NOT_EXPR );

				foreach( cell, expr->args )
				{
					const Node* const node = (const Node*)lfirst( cell );
					context->candidates = merge_candidates( context->candidates,
											scan_generic_node( node, context->opnos,
															context->rangeTableStack));
				}
			}
			else
			{
				/* AND expression */
				List* compositeCandidates = NIL;

				foreach( cell, expr->args )
				{
					const Node* const node = (const Node*)lfirst( cell );
					List	*icList; /* Index candidate list */
					List	*cicList; /* Composite index candidate list */

					icList	= scan_generic_node( node, context->opnos, context->rangeTableStack );
					cicList = build_composite_candidates(candidates, icList);
					context->candidates = merge_candidates(context->candidates, icList);
					compositeCandidates = merge_candidates(compositeCandidates,
															cicList);
				}

				/* now append the composite (multi-col) indexes to the list */
				context->candidates = merge_candidates(context->candidates, compositeCandidates);
			}
			return false;
		}
		break;
		
		/* if the node is an operator */
		case T_OpExpr:
		{
			/* get candidates if operator is supported */
			const OpExpr* const expr = (const OpExpr*)root;
			elog( DEBUG3 , "IND ADV: OpExpr: opno:%d, location:%d",expr->opno,expr->location);

			if( list_member_oid( context->opnos, expr->opno ) )
			{
				bool foundToken = false;

	            /* this part extracts the expr to be used as the predicate for the partial index */
			    elog( DEBUG3 , "IND ADV: OpExpr: check context");

                foreach( cell, expr->args )
                {
                    const Node* const node = (const Node*)lfirst( cell );

                    elog( DEBUG4 , "IND ADV: OpExpr: check var %d",nodeTag(node));
                    if(nodeTag(node)==T_Var){
                        const Var* const e = (const Var*)(node);
                        List* rt = list_nth( context->rangeTableStack, e->varlevelsup );
                        const RangeTblEntry* rte = list_nth( rt, e->varno - 1 );
						if (rte->rtekind == RTE_CTE) break; // break if working on CTE.
                        RelClause* rc = NULL;
						char *token = NULL;
						
                        elog( DEBUG3 , "IND ADV: OpExpr: working on: %s",rte->eref->aliasname);
                        char *varname = get_relid_attribute_name(rte->relid, e->varattno);
                        elog( DEBUG3 , "IND ADV: OpExpr: working on: %d",rte->relid);
			char *token_str = strdup(idxadv_columns);
						
			elog( DEBUG1 , "IND ADV: OpExpr: check right var, %s, cols: %s",varname,idxadv_columns);
			token = strtok(token_str, ",");
			elog( DEBUG1 , "IND ADV: token %s",token);
			while (token && ! foundToken){
				foundToken = (strcmp(token,varname)==0) ? true : false ;
				token = strtok(NULL, ",");
				elog( DEBUG4 , "IND ADV: token %s",token);
			}
                        						
                        if (foundToken)
                        {
                            ListCell* relPredicates = get_rel_clausesCell(table_clauses, rte->relid,rte->eref->aliasname);
                            elog( INFO , "IND ADV: create the clause for: %s",rte->eref->aliasname);
                            if (relPredicates == NULL){
                                Node* f = linitial(expr->args);
                                Node* s = lsecond(expr->args);
                                elog( DEBUG4 , "index candidate - create a new entry for the relation");
                                // create a new entry for the relation
                                rc = (RelClause*)palloc0(sizeof(RelClause));
                                rc->reloid = rte->relid;
                                rc->erefAlias = pstrdup(rte->eref->aliasname);
                                if(nodeTag(f)==T_Var){
                                    rc->predicate = lappend(rc->predicate,(Expr *)makePredicateClause((Expr *) root, (Const*) s, (Var*) f));
                                }else{
                                    rc->predicate = lappend(rc->predicate,(Expr *)makePredicateClause((Expr *) root, (Const*) f, (Var*) s));
                                }
                                elog( DEBUG4 , "IND ADV: created the clause");
                                
                                table_clauses = lappend(table_clauses, rc );
                            }else{
                                //use the existing rc
                                Node* f = linitial(expr->args);
                                Node* s = lsecond(expr->args);
                                elog( DEBUG4 , "index candidate - use the existing rc");
                                rc = (RelClause*)lfirst( relPredicates );
                                if(nodeTag(f)==T_Var){
                                    rc->predicate = lappend(rc->predicate,(Expr *)makePredicateClause((Expr *) root, (Const*) s, (Var*) f));
                                }else{
                                    rc->predicate = lappend(rc->predicate,(Expr *)makePredicateClause((Expr *) root, (Const*) f, (Var*) s));
                                }
                                elog( DEBUG4 , "IND ADV: created the clause");
                            }

                            //context = (QueryContext*)palloc0(sizeof(QueryContext));
                            elog_node_display( DEBUG4 , "predicate", linitial(rc->predicate),true);
                            //context->predicate = make_ands_implicit((Expr *) root);

                            elog( DEBUG4 , "index candidate - context->predicate set");
                            break;
                        }
                    }
                }

				if(! foundToken){
					foreach( cell, expr->args )
					{
						const Node* const node = (const Node*)lfirst( cell );															

						context->candidates = merge_candidates( context->candidates,
												scan_generic_node( node, context->opnos,
																context->rangeTableStack));
					
					}
				}
			}
			return false;
		}
		break;

		/* if this case is reached, the variable is an index-candidate */
		case T_Var:
		{
			const Var* const expr = (const Var*)root;
			List* rt = list_nth( context->rangeTableStack, expr->varlevelsup );
			const RangeTblEntry* rte = list_nth( rt, expr->varno - 1 );			

			elog( DEBUG3 , "index candidate - var: %d rtekind: %d",expr->varattno,rte->rtekind);			

			/* only relations have indexes */
			if( rte->rtekind == RTE_RELATION )
			{
				Relation base_rel = heap_open( rte->relid, AccessShareLock );
				elog( DEBUG3 , "index candidate - here %d %d %d %d %d",RelationNeedsWAL(base_rel),!IsSystemRelation(base_rel),expr->varattno,base_rel->rd_rel->relpages,base_rel->rd_rel->reltuples);
				/* We do not support catalog tables and temporary tables */
				if( RelationNeedsWAL(base_rel)
					&& !IsSystemRelation(base_rel)
					/* and don't recommend indexes on hidden/system columns */
					&& expr->varattno > 0
					/* and it should have at least two tuples */
					//TODO: Do we really need these checks?
					//&& base_rel->rd_rel->relpages > 1
					//&& base_rel->rd_rel->reltuples > 1
					) 
				{
					/* create index-candidate and build a new list */
					int				i;
					IndexCandidate	*cand = (IndexCandidate*)palloc0(
														sizeof(IndexCandidate));

					elog( DEBUG3 , "index candidate - in here");

					cand->varno         = expr->varno;
					cand->varlevelsup   = expr->varlevelsup;
					cand->ncols         = 1;
					cand->reloid        = rte->relid;
					cand->erefAlias     = pstrdup(rte->eref->aliasname);					
					cand->inh			= rte->inh;
					elog( DEBUG3 , "index candidate - rel: %s, inh: %s",cand->erefAlias,BOOL_FMT(rte->inh));
					cand->vartype[ 0 ]  = expr->vartype;
					cand->varattno[ 0 ] = expr->varattno;
					cand->varname[ 0 ]   = get_relid_attribute_name(rte->relid, expr->varattno);
                    elog( DEBUG3 , "index candidate - rel: %s, var: %s",cand->erefAlias,cand->varname[ 0 ]);
					/*FIXME: Do we really need this loop? palloc0 and ncols,
					 * above, should have taken care of this!
					 */
					for( i = 1; i < INDEX_MAX_KEYS; ++i )
						cand->varattno[i] = 0;

					context->candidates = list_make1( cand );
				}

				heap_close( base_rel, AccessShareLock );
			}
			return false;
		}
		break;

		/* Query found */
		case T_Query:
		{
			const Query* const query = (const Query*)root;

			context->candidates = scan_query( query, context->opnos, context->rangeTableStack );
			return false;
		}
		break;
		case T_WindowFunc:
		{
		  elog(DEBUG4, "IDX_ADV: inside window func");
		}break;
		case T_MinMaxExpr:
		{
		  elog(DEBUG4, "IDX_ADV: inside T_MinMaxExpr func");
		}break;
#if PG_VERSION_NUM >= 90400
		case T_GroupingFunc:
		{
		  elog(DEBUG4, "IDX_ADV: inside grouping func");
		}break;
#endif
		/* create functional index */
		case T_FuncExpr:
			{
				int				i;
				bool			too_complex = false;
				IndexCandidate	*cand;
				//IndexCandidate	*cand = (IndexCandidate*)palloc0(sizeof(IndexCandidate));
				IndexElem *ind_elm = palloc0(sizeof(IndexElem));
				FuncExpr* expr = (FuncExpr*) palloc0(sizeof(FuncExpr));
				Node       *func_var; /* use this to find the relation info*/
				
				elog(DEBUG4, "IND ADV: duplicate func expr properties.");
				expr->xpr = ((const FuncExpr*)root)->xpr;
				expr->funcid = ((const FuncExpr*)root)->funcid;
				expr->funcresulttype = ((const FuncExpr*)root)->funcresulttype;
				expr->funcretset = ((const FuncExpr*)root)->funcretset;
				//expr->funcvariadic = ((const FuncExpr*)root)->funcvariadic;
				expr->funcformat = ((const FuncExpr*)root)->funcformat;
				expr->funccollid = ((const FuncExpr*)root)->funccollid;
				expr->inputcollid = ((const FuncExpr*)root)->inputcollid;
				expr->args = list_copy(((const FuncExpr*)root)->args);
				expr->location = ((const FuncExpr*)root)->location;

				elog(DEBUG4, "TBD: support functional indexes.");
				//elog_node_display(DEBUG2,"Func Expr: ",root,true);
				elog_node_display(DEBUG2,"Func Expr: ",expr,true);
				//ind_elm->type = T_FuncExpr;
				//ind_elm->name = NULL;
				//ind_elm->expr = (Node *)expr;
				//ind_elm->indexcolname = NULL;
				//ind_elm->collation = NIL;
				//ind_elm->opclass = NIL; //TODO: change to the opclass needed for varchar_pattern_ops
				//ind_elm->ordering = SORTBY_DEFAULT ;
				//ind_elm->nulls_ordering = SORTBY_NULLS_DEFAULT ;

				

				/* TODO: support func indexes.
				* currently we only support functions with variables on the first parameter.
				* other types of functions will be ignored.
				*/
				//	indexInfo->ii_KeyAttrNumbers[attn] = 0; /* marks expression */
				//  indexInfo->ii_Expressions = lappend(indexInfo->ii_Expressions, expr);
				
				// get to the buttom Var
				elog( DEBUG4 , "index candidate - get to buttom var");
				if(list_length(expr->args)==0) { break;} // don't create indexes for expressions with no variables
				func_var = list_nth(expr->args,0);
				elog( DEBUG4 , "index candidate - get to buttom type: %d",func_var->type);
				elog_node_display(DEBUG2,"Func Expr: - maybe ",func_var,true);
				while((!IsA(func_var, Var))&&(!IsA(func_var, Const)) && !too_complex){
					elog( DEBUG4 , "index candidate - loop to get to buttom type");
					if(IsA(func_var, FuncExpr))
					{
						func_var = list_nth(((FuncExpr *)func_var)->args,0); // we only address first parameter.
					}
					else if(IsA(func_var, OpExpr))
					{
						too_complex = true;
					}
					else
					{
						func_var = (Node *)((RelabelType *) func_var)->arg;
					}
					elog( DEBUG4 , "index candidate - get to buttom type: %d",func_var->type);
				}
				if (too_complex){ break;} // too complex
				elog( DEBUG4 , "index candidate - get to buttom var - check const");
				if (IsA(func_var, Const)){ break;} // don't create indexes for expressions on constants
				elog( DEBUG4 , "index candidate - function on var");
				if (IsA(func_var, Var) &&
                ((Var *) func_var)->varattno != InvalidAttrNumber)
				{
				//	const Var* const expr = (const Var*)expr->args;
					List* rt = list_nth( context->rangeTableStack, ((Var *)func_var)->varlevelsup );
					const RangeTblEntry* rte = list_nth( rt, ((Var *)func_var)->varno - 1 );
					if (rte->rtekind != RTE_RELATION) {break;}
					char *varname = get_attname(rte->relid, ((Var *)func_var)->varattno);
					if (varname == NULL)
						elog(ERROR, "cache lookup failed for attribute %d of relation %u",varname, rte->relid);

					elog( DEBUG4 , "index candidate - function on var: %s",varname);
					cand = (IndexCandidate*)palloc0(sizeof(IndexCandidate));

				///* only relations have indexes */
				//if( rte->rtekind == RTE_RELATION )
				//{
				//	Relation base_rel = heap_open( rte->relid, AccessShareLock );

				//	/* We do not support catalog tables and temporary tables */
				//	if( RelationNeedsWAL(base_rel)
				//		&& !IsSystemRelation(base_rel)
				//		/* and don't recommend indexes on hidden/system columns */
				//		&& expr->varattno > 0
				//		/* and it should have at least two tuples */
				//		//TODO: Do we really need these checks?
				//		&& base_rel->rd_rel->relpages > 1
				//		&& base_rel->rd_rel->reltuples > 1
				//		&& strcmp(varname,IDX_ADV_PART_COL)!=0 )
				//	{
				//		/* create index-candidate and build a new list */
						

				//		cand->varno         = expr->varno;
						cand->varlevelsup   = ((Var *)func_var)->varlevelsup;
						cand->ncols         = 1;
						cand->reloid        = rte->relid;
						cand->erefAlias     = pstrdup(rte->eref->aliasname);
						cand->idxused       = false;
						cand->inh			= rte->inh;

						cand->vartype[ 0 ]  = ((Var *)func_var)->vartype;
				//		cand->varattno[ 0 ] = expr->varattno;
				//		cand->varname[ 0 ]   = get_relid_attribute_name(rte->relid, expr->varattno);
	   //                 elog( DEBUG3 , "index candidate - rel: %s, var: %s",cand->erefAlias,cand->varname[ 0 ]);
				//		/*FIXME: Do we really need this loop? palloc0 and ncols,
				//		 * above, should have taken care of this!
				//		 */
						for( i = 0; i < INDEX_MAX_KEYS; ++i )
							cand->varattno[i] = 0;


						
				//	}

				//	heap_close( base_rel, AccessShareLock );
				}
				//elog_node_display(DEBUG2,"Func Expr pre list make: ",ind_elm,true);
				elog( DEBUG4 , "index candidate - func expr");
				cand->attList = lappend(cand->attList, expr);
				elog_node_display(DEBUG4,"Func Expr: ",cand->attList,true);
				context->candidates = list_make1( cand );
				return false;
			}
			break;	
			/* default:
			{
				elog( DEBUG4 , "IDX ADV: tree walker - reached the default handler ");
				elog(ERROR, "unrecognized node type: %d",(int) nodeTag(root));
			}			
			break; */
	}
	
	elog( DEBUG4, "IND ADV: scan_generic_node: EXIT" );



   // for any root type not specially processed, do:
   return expression_tree_walker(root, index_candidates_walker, (void *) context);
}


/**
* scan_generic_node
* \brief this scanner uses the tree walker to drill down into the query tree to find Indexing candidates.
*/
static List* scan_generic_node(	const Node* const root,
							List* const opnos,
							List* const rangeTableStack )
{
	ScanContext context;
        context.candidates = NIL;
	context.opnos = opnos;
	context.rangeTableStack = rangeTableStack;
	elog( DEBUG4, "IND ADV: scan_generic_node: before tree walk" );
	query_or_expression_tree_walker(root,
	                                index_candidates_walker,
	                                (void *) &context,
	                                0);
	elog( DEBUG4, "IND ADV: scan_generic_node: return index candidates" );
	return context.candidates;

}




/**
 * compare_candidates
 * \brief compares 2 index candidates based on thier OID, alias, and columns
 * \TODO extend to support functional indexes
 */
static int compare_candidates( const IndexCandidate* ic1,
				   const IndexCandidate* ic2 )
{
	int result = (signed int)ic1->reloid - (signed int)ic2->reloid;
	elog( DEBUG3, "IND ADV: compare_candidates: ENTER" );

	if( result == 0 )
	{
		result = strcmp(ic1->erefAlias,ic2->erefAlias );
		if (result == 0)
		{
			result = ic1->ncols - ic2->ncols;

			if( result == 0 )
			{
				int i = 0;

				do
				{
					result = ic1->varattno[ i ] - ic2->varattno[ i ];
					++i;
				} while( ( result == 0 ) && ( i < ic1->ncols ) );
			}
		}
	}

	return result;
}

/*!
 * \brief get the List containing the predicate clauses for a specific rel
 */
static List* get_rel_clauses(List* table_clauses, Oid reloid, char* erefAlias)
{
    ListCell		*cell;
	elog( DEBUG3 , "IND ADV: get_rel_clauses: enter - look for: %s",erefAlias);
    foreach( cell, table_clauses )
	{
		const RelClause* const relClause = (RelClause*)lfirst( cell );
		elog( DEBUG3 , "IND ADV: get_rel_clauses: in loop");
		if ((relClause->reloid == reloid) && (strcmp(relClause->erefAlias,erefAlias)==0)){
		    return relClause->predicate;
		}
    }
	elog( DEBUG3 , "IND ADV: get_rel_clauses: exit - found nothing");
    return NIL;
}

/*!
 * \brief get the ListCell containing the clauses for a specific rel
 */
static ListCell* get_rel_clausesCell(List* table_clauses, Oid reloid, char* erefAlias)
{
    ListCell		*cell;

    elog( DEBUG4 , "IND ADV: get_rel_clausesCell: enter - look for: %s",erefAlias);
    elog( DEBUG4 , "IND ADV: get_rel_clausesCell: looking inlist of len: %d",list_length(table_clauses));
    foreach( cell, table_clauses )
	{
		const RelClause* const relClause = (RelClause*)lfirst( cell );
		elog( DEBUG4 , "IND ADV: get_rel_clausesCell: RelClause");
		elog( DEBUG4 , "IND ADV: get_rel_clausesCell: RelClause: %s",relClause->erefAlias);
		if ((relClause->reloid == reloid) && (strcmp(relClause->erefAlias,erefAlias)==0)){
		    elog( DEBUG4 , "IND ADV: get_rel_clausesCell: found cell");
		    return cell;
		}
    }
    elog( DEBUG4 , "IND ADV: get_rel_clausesCell: found NULL");
    return NULL;
}

/**
 * log_candidates
 */
static void log_candidates( const char* prefix, List* list )
{
	ListCell		*cell;
	StringInfoData	str;/* output string */
    elog( DEBUG4 , "IND ADV: log_candidates: enter");
    /* Verify the list is not empty */
    if(list_length(list)==0){
    	elog( DEBUG4 , "IND ADV: empty list: exit");
    	return;
    }

	/* don't do anything unless we are going to log it */
	/*if( log_min_messages < DEBUG1 )
		return;*/

	initStringInfo( &str );

	foreach( cell, list )
	{
		int i;
		const IndexCandidate* const cand = (IndexCandidate*)lfirst( cell );

		appendStringInfo( &str, " %d_(", cand->reloid );

		for( i = 0; i < cand->ncols; ++i )
			appendStringInfo( &str, "%s%d", (i>0?",":""), cand->varattno[ i ] );

		appendStringInfo( &str, ")%c", ((lnext( cell ) != NULL)?',':' ') );
	}

	elog( DEBUG1 , "IND ADV: %s: |%d| {%s}", prefix, list_length(list),
			str.len ? str.data : "" );

	if( str.len > 0 ) pfree( str.data );
}

/**
 * merge_candidates
 * 		It builds new list out of passed in lists, and then frees the two lists.
 *
 * This function maintains order of the candidates as determined by
 * compare_candidates() function.
 */
static List*
merge_candidates( List* list1, List* list2 )
{
	List *ret;
	ListCell *cell1;
	ListCell *cell2;
	ListCell *prev2;

	if( list_length( list1 ) == 0 && list_length( list2 ) == 0 )
		return NIL;

	elog( DEBUG4, "IND ADV: merge_candidates: ENTER" );
	elog( DEBUG4, "IND ADV: merge_candidates: list 1 length: %d",list_length( list1 ) );
	elog( DEBUG4, "IND ADV: merge_candidates: list 2 length: %d",list_length( list2 ) );

	/* list1 and list2 are assumed to be sorted in ascending order */

	elog( DEBUG1, "IND ADV: ---merge_candidates---" );
	log_candidates( "idxcd-list1", list1 );
	log_candidates( "idxcd-list2", list2 );	

	if( list_length( list1 ) == 0 )
		return list2;

	if( list_length( list2 ) == 0 )
		return list1;

        if ( list1 == list2 ) return list1;

	ret = NIL;
	prev2 = NULL;	

	for( cell1 = list_head(list1), cell2 = list_head(list2);
		(cell1 != NULL) && (cell2 != NULL); )
	{
		const int cmp = compare_candidates( (IndexCandidate*)lfirst( cell1 ),
				  		    (IndexCandidate*)lfirst( cell2 ) );
                elog( DEBUG4, "IDX_ADV: candidate compare returns: %d",cmp);
		if( cmp <= 0 )
		{
			/* next candidate comes from list 1 */
			ret = lappend( ret, lfirst( cell1 ) );

			cell1 = lnext( cell1 );

			/* if we have found two identical candidates then we remove the
			 * candidate from list 2
			 */
			if( cmp == 0 )
			{
				ListCell *next = lnext( cell2 );

				//pfree( (IndexCandidate*)lfirst( cell2 ) );
				//list2 = list_delete_cell( list2, cell2, prev2 );

				cell2 = next;
			}
		}
		else
		{
			/* next candidate comes from list 2 */
			ret = lappend( ret, lfirst( cell2 ) );

			prev2 = cell2;
			cell2 = lnext( cell2 );
		}
	}
        
        elog( DEBUG4, "IDX_ADV: so far we have: %d",list_length( ret ));
         log_candidates( "so far: ", ret );

	/* Now append the leftovers from both the lists; only one of them should have any elements left */
	for( ; cell1 != NULL; cell1 = lnext(cell1) )
		ret = lappend( ret, lfirst(cell1) );

	for( ; cell2 != NULL ; cell2 = lnext(cell2) )
		ret = lappend( ret, lfirst(cell2) );

        elog( DEBUG4, "IDX_ADV: free current candidate lists");
        list1->type = T_List;
        list2->type = T_List;
        elog( DEBUG4, "IDX_ADV: free current candidate list1");
	if (list1 != NIL ) list_free( list1 );
        elog( DEBUG4, "IDX_ADV: free current candidate list2");
	if (list2 != NIL ) list_free( list2 );

	log_candidates( "merged-list", ret );

	elog( DEBUG4, "IND ADV: merge_candidates: EXIT" );
        ret->type = T_List;
	return ret;
}

/**
 * expand_inherited_candidates
 * 		It builds new list out of passed in list, to expand inherited tables.
 *
 * see: expand_inherited_rtentry
 */
static List* expand_inherited_candidates(List* list)
{
	ListCell *cell;
	LOCKMODE    lockmode = NoLock;
	IndexCandidate *cand;
	List       *inhOIDs;
	List       *newCandidates = NIL;
	ListCell   *l;

	elog(DEBUG3,"expand_inherited_candidates: Enter - length: %d",list_length(list));
	for( cell = list_head(list); (cell != NULL) ; cell = lnext( cell ))
	{
		cand = ((IndexCandidate*)lfirst( cell ));
		newCandidates = lappend(newCandidates, cand);
		if(!cand->inh)
		{
			elog(DEBUG3,"expand_inherited_candidates: not inh skipping");
			continue;
		}
		elog(DEBUG3,"expand_inherited_candidates: inh expending");
		/* Scan for all members of inheritance set, acquire needed locks */
		//inhOIDs = find_all_inheritors(cand->reloid, lockmode, NULL);
		inhOIDs = find_inheritance_children(cand->reloid, lockmode);
		/* Check that there's at least one descendant, else treat as no-child
         * case.  This could happen despite above has_subclass() check, if table
         * once had a child but no longer does.
         */
		//if (list_length(inhOIDs) < 2)
		if (list_length(inhOIDs) < 1)
		{
			/* Clear flag before returning */
			elog(DEBUG3,"expand_inherited_candidates: not enough inh -> skipping");
			cand->inh = false;
			continue;
		}

		elog(DEBUG3,"expand_inherited_candidates: loop over sons: %d ",list_length(inhOIDs));
		foreach(l, inhOIDs)
		{
			int i;
			Oid         childOID = lfirst_oid(l);
			IndexCandidate* cic = (IndexCandidate*)palloc(sizeof(IndexCandidate));
			//Relation base_rel = heap_open( childOID, AccessShareLock );
			/* init some members of composite candidate 1 */
			cic->varno			= -1;
			cic->varlevelsup	= -1;
			cic->ncols			= cand->ncols;
			cic->reloid			= childOID;
			cic->erefAlias      = pstrdup(cand->erefAlias);
			cic->idxused		= false;
			cic->parentOid		= cand->reloid;
			
            elog( DEBUG3, "expand_inherited_candidates: start att copy, ncols: %d", cand->ncols);
			/* copy attributes of the candidate to the inherited candidate 	*/
			for( i = 0; i < cand->ncols; ++i)
			{
				cic->vartype[ i ]	= cand->vartype[ i ];
				cic->varattno[ i ] = cand->varattno[ i ];
				cic->varname[ i ] = cand->varname[ i ];
			}							

			/* set remaining attributes to null */
			for( i = cand->ncols ;	i < INDEX_MAX_KEYS;	++i )
			{
				cic->varattno[ i ] = 0;
							
			}

			/* cope index experessions for the new composite indexes */
			cic->attList = list_copy(cand->attList);
			elog(DEBUG3,"expand_inherited_candidates: start att copy,attlist cic: %d ",list_length(cic->attList));
			newCandidates = lappend(newCandidates, cic);
							
		}
	}
	
	list_free(list);
	elog(DEBUG3,"expand_inherited_candidates: Exit - length: %d",list_length(newCandidates));
	return newCandidates;
}


/**
* \brief expand the inherited relation clause for the table_clauses list.
**/
static void expand_inherited_rel_clauses()
{
	ListCell *cell;
	LOCKMODE    lockmode = NoLock;
	RelClause *cand;
	List       *inhOIDs;	
	ListCell   *l;

	elog(DEBUG3,"expand_inherited_rel_clauses: Enter - length: %d",list_length(table_clauses));
	for( cell = list_head(table_clauses); (cell != NULL) ; cell = lnext( cell ))
	{
		cand = ((RelClause*)lfirst( cell ));
		
		elog(DEBUG3,"expand_inherited_rel_clauses: inh expending");
		/* Scan for all members of inheritance set, acquire needed locks */
		//inhOIDs = find_all_inheritors(cand->reloid, lockmode, NULL);
		inhOIDs = find_inheritance_children(cand->reloid, lockmode);
		/* Check that there's at least one descendant, else treat as no-child
         * case.  This could happen despite above has_subclass() check, if table
         * once had a child but no longer does.
         */
		//if (list_length(inhOIDs) < 2)
		if (list_length(inhOIDs) < 1)
		{
			/* Clear flag before returning */
			elog(DEBUG3,"expand_inherited_rel_clauses: not enough inh -> skipping");			
			continue;
		}

		elog(DEBUG3,"expand_inherited_rel_clauses: loop over sons: %d ",list_length(inhOIDs));
		foreach(l, inhOIDs)
		{
			//int i;
			Oid         childOID = lfirst_oid(l);
			RelClause* cic = (RelClause*)palloc(sizeof(RelClause));			
			
			//Relation base_rel = heap_open( childOID, AccessShareLock );
			/* init some members of composite candidate 1 */			
			cic->reloid			= childOID;
			cic->erefAlias      = cand->erefAlias; 		
			elog(DEBUG3,"expand_inherited_rel_clauses: create chield clause for %d, name: %s",childOID,cic->erefAlias);
			            			
			/* cope table clause experessions for the new cheild table */
			cic->predicate = list_copy(cand->predicate);			
			table_clauses = lappend(table_clauses, cic);							
		}
	}		
	elog(DEBUG3,"expand_inherited_rel_clauses: Exit - length: %d",list_length(table_clauses));	
}


/**
 * build_composite_candidates.
 *
 * @param [IN] list1 is a sorted list of candidates in ascending order.
 * @param [IN] list2 is a sorted list of candidates in ascending order.
 *
 * @returns A new sorted list containing composite candidates.
 */
static List*
build_composite_candidates( List* list1, List* list2 )
{
	ListCell *cell1 = list_head( list1 );
	ListCell *cell2 = list_head( list2 );
	IndexCandidate *cand1;
	IndexCandidate *cand2;

	List* compositeCandidates = NIL;

	elog( DEBUG4, "IND ADV: build_composite_candidates: ENTER" );

	if( cell1 == NULL || cell2 == NULL )
		goto DoneCleanly;

	elog( DEBUG4, "IND ADV: ---build_composite_candidates---" );
	log_candidates( "idxcd-list1", list1 );
	log_candidates( "idxcd-list2", list2 );

		/* build list with composite candiates */
	while( ( cell1 != NULL ) && ( cell2 != NULL ) )
	{
		int cmp ;

		cand1 = ((IndexCandidate*)lfirst( cell1 ));
		cand2 = ((IndexCandidate*)lfirst( cell2 ));

        /*!
         * \TODO: fix the comparisson to create composite indexes
         */
        elog( DEBUG4, "IND ADV: build_composite_candidates: compare reloids %d %d", cand1->reloid, cand2->reloid);
		cmp = cand1->reloid - cand2->reloid;
		elog( DEBUG4, "IND ADV: build_composite_candidates: compare aliases %s %s",cand1->erefAlias,cand2->erefAlias );
		cmp = cmp + strcmp(cand1->erefAlias,cand2->erefAlias);

		if( cmp != 0 )
		{
			Oid relOid;

			if( cmp < 0 )
			{
				/* advance in list 1 */
				relOid = cand2->reloid;

				do
					cell1 = lnext( cell1 );
				while( cell1 != NULL && (relOid > cand1->reloid));
			}
			else
			{
				/* advance in list 2 */
				relOid = cand1->reloid;

				do
					cell2 = lnext( cell2 );
				while( cell2 != NULL && ( relOid > cand2->reloid ));
			}
		}
		else
		{
			/* build composite candidates */
			Oid relationOid = ((IndexCandidate*)lfirst(cell1))->reloid;
			char* alias = ((IndexCandidate*)lfirst(cell1))->erefAlias;
			ListCell* l1b;

			elog( DEBUG3, "IND ADV: build_composite_candidates: build composite candidates %s ",alias );

			do
			{
				cand2 = lfirst( cell2 );

				l1b = cell1;
				do
				{
					cand1 = lfirst( l1b );

					/* do not build a composite candidate if the number of
					 * attributes would exceed INDEX_MAX_KEYS
					*/
					if(( ( cand1->ncols + cand2->ncols ) < INDEX_MAX_KEYS )&&(( cand1->ncols + cand2->ncols ) <= idxadv_composit_max_cols))
					{

						/* Check if candidates have any common attribute */
						int		i1, i2;
						bool	foundCommon = false;

						for(i1 = 0; i1 < cand1->ncols && !foundCommon; ++i1)
							for(i2 = 0; i2 < cand2->ncols && !foundCommon; ++i2)
								if(cand1->varattno[i1] == cand2->varattno[i2])
									foundCommon = true;
						if( foundCommon )
							elog( DEBUG3, "IND ADV: build_composite_candidates: found common - %d - skipping ",cand1->varattno[i1] );

						/* build composite candidates if the previous test
						 * succeeded
						 */
						if( !foundCommon )
						{
							signed int cmp;

							/* composite candidate 1 is a combination of
							 * candidates 1,2 AND
							 * composite candidate 2 is a combination of
							 * candidates 2,1
							 */
							IndexCandidate* cic1
								= (IndexCandidate*)palloc(
													sizeof(IndexCandidate));
							IndexCandidate* cic2
								= (IndexCandidate*)palloc(
													sizeof(IndexCandidate));

							/* init some members of composite candidate 1 */
							cic1->varno			= -1;
							cic1->varlevelsup	= -1;
							cic1->ncols			= cand1->ncols + cand2->ncols;
							cic1->reloid		= relationOid;
							cic1->erefAlias     = pstrdup(alias);
							cic1->idxused		= false;

							/* init some members of composite candidate 2 */
							cic2->varno			= -1;
							cic2->varlevelsup	= -1;
							cic2->ncols			= cand1->ncols + cand2->ncols;
							cic2->reloid		= relationOid;
							cic2->erefAlias     = pstrdup(alias);
							cic2->idxused		= false;

                            elog( DEBUG3, "IND ADV: build_composite_candidates: start att copy, ncols1: %d, ncols2: %d - total: %d", cand1->ncols , cand2->ncols,cic2->ncols);
							/* copy attributes of candidate 1 to attributes of
							 * composite candidates 1,2
							 */
							for( i1 = 0; i1 < cand1->ncols; ++i1)
							{
								cic1->vartype[ i1 ]
									= cic2->vartype[cand2->ncols + i1]
									= cand1->vartype[ i1 ];

								cic1->varattno[ i1 ]
									= cic2->varattno[cand2->ncols + i1]
									= cand1->varattno[ i1 ];
								cic1->varname[ i1 ]
				                    = cic2->varname[cand2->ncols + i1]
                                	= cand1->varname[ i1 ];
							}

							/* copy attributes of candidate 2 to attributes of
							 * composite candidates 2,1
							 */
							for( i1 = 0; i1 < cand2->ncols; ++i1)
							{
								cic1->vartype[cand1->ncols + i1]
									= cic2->vartype[ i1 ]
									= cand2->vartype[ i1 ];

								cic1->varattno[cand1->ncols + i1]
									= cic2->varattno[ i1 ]
									= cand2->varattno[ i1 ];
								
								cic1->varname[cand1->ncols + i1]
                                    = cic2->varname[ i1 ]
				                    = cand2->varname[ i1 ];
							}

							/* set remaining attributes to null */
							for( i1 = cand1->ncols + cand2->ncols;
									i1 < INDEX_MAX_KEYS;
									++i1 )
							{
								cic1->varattno[ i1 ] = 0;
								cic2->varattno[ i1 ] = 0;
							}

							/* cope index experessions for the new composite indexes */
							cic1->attList = list_concat_unique(cand1->attList,cand2->attList);
							cic2->attList = list_concat_unique(cand2->attList,cand1->attList);
							elog(DEBUG3,"build_composite_candidates: start att copy,attlist cic1: %d, cic2: %d ",list_length(cic1->attList),list_length(cic2->attList));
							
							/* add new composite candidates to list */
							cmp = compare_candidates(cic1, cic2);

							if( cmp == 0 )
							{
								compositeCandidates =
									merge_candidates( list_make1( cic1 ),
													compositeCandidates );
								pfree( cic2 );
							}
							else
							{
								List* l;

								if( cmp < 0 )
									l = lcons( cic1, list_make1( cic2 ) );
								else
									l = lcons( cic2, list_make1( cic1 ) );

								compositeCandidates =
									merge_candidates(l, compositeCandidates);
							}
						}
					}

					l1b = lnext( l1b );

				} while( ( l1b != NULL ) &&
					( relationOid == ((IndexCandidate*)lfirst( l1b ))->reloid));

				cell2 = lnext( cell2 );

			} while( ( cell2 != NULL ) &&
				( relationOid == ((IndexCandidate*)lfirst( cell2 ))->reloid ) );
			cell1 = l1b;
		}
	}

	log_candidates( "composite-l", compositeCandidates );

DoneCleanly:
	elog( DEBUG4, "IND ADV: build_composite_candidates: EXIT" );

	return compositeCandidates;
}

/**
 * create_virtual_indexes
 *    creates an index for every entry in the index-candidate-list.
 *
 * It may delete some candidates from the list passed in to it.
 */
static List* create_virtual_indexes( List* candidates )
{
	ListCell	*cell;					  /* an entry from the candidate-list */
	ListCell	*prev, *next;						 /* for list manipulation */
	char		idx_name[ 16 ];		/* contains the name of the current index */
	int		idx_count = 0;				   /* number of the current index */
	IndexInfo*	indexInfo;
	Oid		op_class[INDEX_MAX_KEYS];			/* the field op class family */
	Oid		collationObjectId[INDEX_MAX_KEYS];	/* the field collation */

	elog( DEBUG4, "IND ADV: create_virtual_indexes: ENTER" );

	/* fill index-info */
	indexInfo = makeNode( IndexInfo );

	indexInfo->ii_Expressions		= NIL;
	indexInfo->ii_ExpressionsState	= NIL;
	indexInfo->ii_PredicateState	= NIL;
	indexInfo->ii_Unique			= false;
	indexInfo->ii_Concurrent		= true;
    indexInfo->ii_ReadyForInserts   = false;
	indexInfo->ii_BrokenHotChain    = false;
	indexInfo->ii_ExclusionOps      = NULL;
	indexInfo->ii_ExclusionProcs    = NULL;
	indexInfo->ii_ExclusionStrats   = NULL;

	/* create index for every list entry */
	/* TODO: simplify the check condition of the loop; it is basically
	 * advancing the 'next' pointer, so maybe this will work
	 * (next=lnext(), cell()); Also, advance the 'prev' pointer in the loop
	 */
	elog( DEBUG1, "IND ADV: create_virtual_indexes: number of cand: %d", list_length(candidates) );

	for( prev = NULL, cell = list_head(candidates);
			(cell && (next = lnext(cell))) || cell != NULL;
			cell = next)
	{
		int			i;

		IndexCandidate* const cand = (IndexCandidate*)lfirst( cell );
		List *colNames=NIL;

		indexInfo->ii_NumIndexAttrs = cand->ncols;
		elog( DEBUG3, "IND ADV: create_virtual_indexes: pre predicate %d, %s, %s", cand->reloid,cand->erefAlias,cand->varname[0]);

		indexInfo->ii_Predicate	    = get_rel_clauses(table_clauses, cand->reloid,cand->erefAlias); /* use the clause found for the relevant relation. */
		elog_node_display( DEBUG4 , "index_create", (Node*)indexInfo->ii_Predicate,true);
		//  indexInfo->ii_Predicate	    = NIL;
		//	indexInfo->ii_KeyAttrNumbers[attn] = 0; /* marks expression */
		elog( DEBUG4, "IND ADV: create_virtual_indexes: add the predicate list to the index, length %d, ncols: %d", list_length(cand->attList),cand->ncols);
		
		indexInfo->ii_Expressions = list_concat_unique(indexInfo->ii_Expressions, cand->attList);

		//elog_node_display( DEBUG2 , "index_create - func: ", (Node*)indexInfo->ii_Expressions,true);

		for( i = 0; i < cand->ncols; ++i )
		{
			elog( DEBUG4, "IND ADV: create_virtual_indexes: prepare op_class[] vartype: %d", cand->vartype[ i ]);
			/* prepare op_class[] */
			collationObjectId[i] = 0;
			op_class[i] = GetDefaultOpClass( cand->vartype[ i ], BTREE_AM_OID );
			/* Replace text_ops with text_pattern_ops */
			if (op_class[i]==3126){
				idxadv_text_pattern_ops?op_class[i] = 10049:NULL; 
                                //  see pg_opclass.oid - this actually works, changes to text_pattern_ops instead of pattern ops (in te strangest way ever... see: http://doxygen.postgresql.org/indxpath_8c_source.html#l03403)
				// TODO: find a way to get this via SYSCACHE instead of fixed numbers (or at least make CONSTS)
				collationObjectId[i] = DEFAULT_COLLATION_OID ; //100; // need to figure this out - 100 is the default but doesn't pass for some reason...
			}
			

			if( op_class[i] == InvalidOid )
				/* don't create this index if couldn't find a default operator*/
				break;

			/* ... and set indexed attribute number */
			indexInfo->ii_KeyAttrNumbers[i] = cand->varattno[i];
			colNames = lappend(colNames,cand->varname[i]);
			elog( DEBUG3, "col: %d, attrno: %d, opclass: %d", cand->varname[i],cand->varattno[i],op_class[i]);
		}
		elog( DEBUG4, "IND ADV: create_virtual_indexes: pre create" );

		/* if we decided not to create the index above, try next candidate */
		if( i < cand->ncols )
		{
			candidates = list_delete_cell( candidates, cell, prev );
			continue;
		}

		/* generate indexname */
		/* FIXME: This index name can very easily collide with any other index
		 * being created simultaneously by other backend running index adviser.
		*/
		sprintf( idx_name, "idx_adv_%d", idx_count );
        elog( DEBUG4, "IND ADV: create_virtual_indexes: pre create" );
        elog( DEBUG4, "idxx name: %s", idx_name );

        elog( DEBUG4, "IND ADV: create_virtual_indexes: open relation" );
		// CHECK: get Relation from  cand->reloid
		Relation relation = heap_open( cand->reloid, AccessShareLock );
        elog( DEBUG4, "IND ADV: create_virtual_indexes: create the index" );
		
		/* create the index without data */
		cand->idxoid = index_create( relation
					, idx_name
					,InvalidOid
					, InvalidOid
					, indexInfo
					,colNames
					, BTREE_AM_OID
					,InvalidOid
					, collationObjectId
					,op_class
					, NULL
					, (Datum)0 //reloptions
					,false //isprimary
					, false //isconstraint
					, false // deferrable
					, false //initdeferred
					, false // allow_system_table_mods
					,true // skip build
#if PG_VERSION_NUM >= 90300
					,true	// 	concurrent
#endif
					,false //is_internal true/false?
#if PG_VERSION_NUM >= 90500
					,false // if_not_exists
#endif
					);

		// TODO: update candidate fields:
		for( i = 0; i < cand->ncols; ++i )
		{
			cand->op_class[i] = op_class[i];
			cand->collationObjectId[i] = collationObjectId[i];
		}
		

		elog( DEBUG4, "IND ADV: virtual index created: oid=%d name=%s size=%d",
					cand->idxoid, idx_name, cand->pages );
	
		/* close the heap */
        heap_close(relation, AccessShareLock);
		elog( DEBUG4, "IND ADV: create_virtual_indexes: numindex %d",list_length( RelationGetIndexList(relation)));

		/* increase count for the next index */
		++idx_count;
		prev = cell;
	}

	pfree( indexInfo );

	/* do CCI to make the new metadata changes "visible" */
	CommandCounterIncrement();

	elog( DEBUG1, "IND ADV: create_virtual_indexes: EXIT" );

	return candidates;
}
/**
 * drop_virtual_indexes
 *    drops all virtual-indexes
 */
static void drop_virtual_indexes( List* candidates )
{
	ListCell* cell;		/* a entry from the index-candidate-list */

	elog( DEBUG1, "IND ADV: drop_virtual_indexes: ENTER" );

	/* drop index for every list entry */
	foreach( cell, candidates )
	{
		/* TODO: have a look at implementation of index_drop! citation:
		 * "NOTE: this routine should now only be called through
		 * performDeletion(), else associated dependencies won't be cleaned up."
		 */

		/* disabling index_drop() call, since it acquires AccessExclusiveLock
		 * on the base table, and hence causing a deadlock when multiple
		 * clients are running the same query
		 */

		IndexCandidate* cand = (IndexCandidate*)lfirst( cell );
		elog( DEBUG1, "IND ADV: dropping virtual index: oid=%d", cand->idxoid );
		//index_drop( cand->idxoid,true );
		elog( DEBUG1, "IND ADV: virtual index dropped: oid=%d", cand->idxoid );
	}

	/* do CCI to make the new metadata changes "visible" */
	CommandCounterIncrement();

	elog( DEBUG3, "IND ADV: drop_virtual_indexes: EXIT" );
}

static 	BlockNumber estimate_index_pages(Oid rel_oid, Oid ind_oid )
{
	Size	data_length;
	int		i;
	int		natts;
	int8	var_att_count;
	BlockNumber rel_pages;						/* diskpages of heap relation */
	float4	rel_tuples;						/* tupes in the heap relation */
	double	idx_pages;					   /* diskpages in index relation */

	TupleDesc			ind_tup_desc;
	Relation			base_rel;
	Relation			index_rel;
	Form_pg_attribute	*atts;

	base_rel	= heap_open( rel_oid, AccessShareLock );
	index_rel	= index_open( ind_oid, AccessShareLock );

	// rel_pages = base_rel->rd_rel->relpages;
	rel_tuples = base_rel->rd_rel->reltuples;
        rel_pages = RelationGetNumberOfBlocks(base_rel);
        elog(DEBUG3, "IDX_ADV: estimate_index_pages: rel_id: %d, pages: %d,, tuples: %f",rel_oid,rel_pages,rel_tuples);

	ind_tup_desc = RelationGetDescr( index_rel );

	atts = ind_tup_desc->attrs;
	natts = ind_tup_desc->natts;


	/*
	 * These calculations are heavily borrowed from index_form_tuple(), and
	 * heap_compute_data_size(). The only difference is that, that they have a
	 * real tuple being inserted, and hence all the VALUES are available,
	 * whereas, we don't have any of them available here.
	 */

	/*
	 * First, let's calculate the contribution of fixed size columns to the size
	 * of index tuple
	 */
	var_att_count = 0;
	data_length = 0;
        elog(DEBUG3, "IDX_ADV: estimate_index_pages: natts: %d",natts);
	for( i = 0; i < natts; ++i)
	{
		/* the following is based on att_addlength() macro */
		if( atts[i]->attlen > 0 )
		{
			/* No need to do +=; RHS is incrementing data_length by including it in the sum */
			data_length = att_align_nominal(data_length, atts[i]->attalign);
			data_length += atts[i]->attlen;
                        elog(DEBUG3, "IDX_ADV: estimate_index_pages: data_length: %d",data_length);
		}
		else if( atts[i]->attlen == -1 )
		{
			data_length += atts[i]->atttypmod + VARHDRSZ;
		}
		else
		{	/* null terminated data */
			Assert( atts[i]->attlen == -2 );
			++var_att_count;
		}
	}

	/*
	 * Now, estimate the average space occupied by variable-length columns, per
	 * tuple. This is calculated as:
	 *     Total 'available' space
	 *       minus space consumed by ItemIdData
	 *       minus space consumed by fixed-length columns
	 *
	 * This calculation is very version specific, so do it for every major release.
	 * TODO: Analyze it for at least 1 major release and document it (perhaps
	 * 			branch the code if it deviates to a later release).
	 */
	if( var_att_count )
		data_length += (((float)rel_pages * (BLCKSZ - (sizeof(PageHeaderData)
					- sizeof(ItemIdData)
				)	)	)
				- (rel_tuples * sizeof(ItemIdData))
				- (data_length * rel_tuples)
			)
				/rel_tuples;

	/* Take into account the possibility that we might have NULL values */
	data_length += IndexInfoFindDataOffset( INDEX_NULL_MASK );

        elog(DEBUG3, "IDX_ADV: estimate_index_pages: data_length: %d",data_length);
        elog(DEBUG3, "IDX_ADV: estimate_index_pages: sizeof(ItemIdData): %d",sizeof(ItemIdData));
        elog(DEBUG3, "IDX_ADV: estimate_index_pages: rel_tuples: %f",rel_tuples);
        elog(DEBUG3, "IDX_ADV: estimate_index_pages: SizeOfPageHeaderData: %d",SizeOfPageHeaderData);
        elog(DEBUG3, "IDX_ADV: estimate_index_pages: BLCKSZ: %d",BLCKSZ);
        elog(DEBUG3, "IDX_ADV: estimate_index_pages: sizeof(BTPageOpaqueData: %d",sizeof(BTPageOpaqueData));

	idx_pages = (rel_tuples * (data_length + sizeof(ItemIdData)))
				/((BLCKSZ - SizeOfPageHeaderData
						- sizeof(BTPageOpaqueData)
					)
					* ((float)BTREE_DEFAULT_FILLFACTOR/100));
	//idx_pages = ceil( idx_pages );

	heap_close( base_rel, AccessShareLock );
	index_close( index_rel, AccessShareLock );

        elog(DEBUG3, "IDX_ADV: estimate_index_pages: idx_pages: %d, %d",(int8)lrint(idx_pages), (BlockNumber)lround(idx_pages));
	return (BlockNumber)lrint(idx_pages);
}


static Expr* makePredicateClause(OpExpr* root,Const* constArg, Var* VarArg)
{
    elog(DEBUG4, "IND ADV: makePredicateClause: Enter");
    VarArg->varno=1;
    return make_opclause(root->opno, root->opresulttype, root->opretset,
                           VarArg, constArg,
                           root->opcollid, root->inputcollid);
}



/*
 * build_index_tlist
 *
 * Build a targetlist representing the columns of the specified index.
 * Each column is represented by a Var for the corresponding base-relation
 * column, or an expression in base-relation Vars, as appropriate.
 *
 * There are never any dropped columns in indexes, so unlike
 * build_physical_tlist, we need no failure case.
 *
 * taken from: src/backend/optimizer/util/plancat.c (PostgreSQL source code)
 *
 */
static List *build_index_tlist(PlannerInfo *root, IndexOptInfo *index, Relation heapRelation)
{
        List       *tlist = NIL;
        Index           varno = index->rel->relid;
        ListCell   *indexpr_item;
        int                     i;
		
		elog(DEBUG1, "build_index_tlist: Enter, ncols: %d, indexpr: %d",index->ncolumns,list_length(index->indexprs));
        indexpr_item = list_head(index->indexprs);
        for (i = 0; i < index->ncolumns; i++)
        {
                int         indexkey = index->indexkeys[i];
                Expr       *indexvar;

				elog(DEBUG4, "build_index_tlist: in loop indexkey: %d",indexkey);
                if (indexkey != 0)
                {
                        /* simple column */
                        Form_pg_attribute att_tup;

                        if (indexkey < 0)
                                att_tup = SystemAttributeDefinition(indexkey,
                                                                    heapRelation->rd_rel->relhasoids);
                        else
                                att_tup = heapRelation->rd_att->attrs[indexkey - 1];

                        indexvar = (Expr *) makeVar(varno,
                                                    indexkey,
                                                    att_tup->atttypid,
                                                    att_tup->atttypmod,
                                                    att_tup->attcollation,
                                                    0);
                }
                else
                {
                        /* expression column */
                        if (indexpr_item == NULL)
                                elog(ERROR, "wrong number of index expressions - expressions column not defined properly");
                        indexvar = (Expr *) lfirst(indexpr_item);						
                        indexpr_item = lnext(indexpr_item);
						elog(DEBUG4, "build_index_tlist: in loop advance  indexpr_item");
						if (indexpr_item != NULL){
							elog(DEBUG4, "   more to advance...");							
						}

                }

                tlist = lappend(tlist,
                                makeTargetEntry(indexvar,
                                                i + 1,
                                                NULL,
                                                false));
        }
        if (indexpr_item != NULL){			
            elog(ERROR, "wrong number of index expressions - ncols not setup properly");
				
		}

        return tlist;
}
