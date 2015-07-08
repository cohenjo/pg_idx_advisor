/*!-------------------------------------------------------------------------
 *
 * \file utils.c
 * \brief utility functions taken from PostgreSQL src code as is.
 *
 * these functions were hidden behind assertions and were copied here to by pass these assertions.
 *
 * Version History:
 * ================
 * created by Cohen Jony
 * 29/07/2013   Jony Cohen      taken from PostgreSQL 9.2.4
 *
 *-------------------------------------------------------------------------
 */

/* ------------------------------------------------------------------------
 * includes (ordered alphabetically)
 * ------------------------------------------------------------------------
 */
#include "utils.h"
/*
 * var_eq_const --- eqsel for var = const case
 *
 * This is split out so that some other estimation functions can use it.
 */

/**
* \brief this function computes the selectivity of 'var=const' clause.
* Note: this code was taken from PostgreeSQL source code to by pass the verifications made in the code.
*/
extern double var_eq_cons(VariableStatData *vardata, Oid operator, Datum constval, bool constisnull, bool varonleft)
{
        double          selec;
        bool            isdefault;

		elog(DEBUG4, "IND ADV: var_eq_cons: Enter");
		
        /*
         * If the constant is NULL, assume operator is strict and return zero, ie,
         * operator will never return TRUE.
         */
        if (constisnull)
                return 0.0;

        /*
         * If we matched the var to a unique index or DISTINCT clause, assume
         * there is exactly one match regardless of anything else.      (This is
         * slightly bogus, since the index or clause's equality operator might be
         * different from ours, but it's much more likely to be right than
         * ignoring the information.)
         */
        if (vardata->isunique && vardata->rel && vardata->rel->tuples >= 1.0)
                return 1.0 / vardata->rel->tuples;

		elog(DEBUG4, "IND ADV: var_eq_cons: non unique - look at stats");		
        if (HeapTupleIsValid(vardata->statsTuple))
        {
                Form_pg_statistic stats;
                Datum      *values;
                int                     nvalues;
                float4     *numbers;
                int                     nnumbers;
                bool            match = false;
                int                     i;

				elog(DEBUG4, "IND ADV: var_eq_cons: get stats tuple struct");
                stats = (Form_pg_statistic) GETSTRUCT(vardata->statsTuple);

				elog(DEBUG4, "IND ADV: var_eq_cons: check for common values");
                /*
                 * Is the constant "=" to any of the column's most common values?
                 * (Although the given operator may not really be "=", we will assume
                 * that seeing whether it returns TRUE is an appropriate test.  If you
                 * don't like this, maybe you shouldn't be using eqsel for your
                 * operator...)
                 */
                if (get_attstatsslot(vardata->statsTuple,
                                    vardata->atttype, vardata->atttypmod,
                                    STATISTIC_KIND_MCV, InvalidOid,
                                    NULL,
                                    &values, &nvalues,
                                    &numbers, &nnumbers))
                {
                       FmgrInfo        eqproc;

					   elog(DEBUG4, "IND ADV: var_eq_cons: check common - get context for operator: %d",operator);
					   elog(DEBUG4, "IND ADV: var_eq_cons: check common - get context for opcode: %d",get_opcode(operator));
                        fmgr_info(get_opcode(operator), &eqproc);

						elog(DEBUG4, "IND ADV: var_eq_cons: loop over vals, var on left? %s",BOOL_FMT(varonleft));
                        for (i = 0; i < nvalues; i++)
                        {
                                /* be careful to apply operator right way 'round */
                                if (varonleft)
                                        match = DatumGetBool(FunctionCall2Coll(&eqproc,
                                                                                DEFAULT_COLLATION_OID,
                                                                                        values[i],
                                                                                        constval));
                                else
                                        match = DatumGetBool(FunctionCall2Coll(&eqproc,
                                                                                DEFAULT_COLLATION_OID,
                                                                                        constval,
                                                                                        values[i]));
                                if (match)
                                        break;
                        }
                }
                else
                {
						elog(DEBUG4, "IND ADV: var_eq_cons: no most-common-value info available");
                        /* no most-common-value info available */
                        values = NULL;
                        numbers = NULL;
                        i = nvalues = nnumbers = 0;
                }

                if (match)
                {
                        /*
                         * Constant is "=" to this common value.  We know selectivity
                         * exactly (or as exactly as ANALYZE could calculate it, anyway).
                         */
                        selec = numbers[i];
						elog(DEBUG4, "IND ADV: var_eq_cons: Constant is \"=\" to this common value");
                }
                else
                {
                        /*
                         * Comparison is against a constant that is neither NULL nor any
                         * of the common values.  Its selectivity cannot be more than
                         * this:
                         */
                        double          sumcommon = 0.0;
                        double          otherdistinct;

						elog(DEBUG4, "IND ADV: var_eq_cons: Comparison is against a constant that is neither NULL nor common value");
                        for (i = 0; i < nnumbers; i++)
                                sumcommon += numbers[i];
                        selec = 1.0 - sumcommon - stats->stanullfrac;
                        CLAMP_PROBABILITY(selec);

                        /*
                         * and in fact it's probably a good deal less. We approximate that
                         * all the not-common values share this remaining fraction
                         * equally, so we divide by the number of other distinct values.
                         */
                        otherdistinct = get_variable_numdistinct(vardata, &isdefault) - nnumbers;
                        if (otherdistinct > 1)
                                selec /= otherdistinct;

                        /*
                         * Another cross-check: selectivity shouldn't be estimated as more
                         * than the least common "most common value".
                         */
                        if (nnumbers > 0 && selec > numbers[nnumbers - 1])
                                selec = numbers[nnumbers - 1];
                }
				elog(DEBUG4, "IND ADV: var_eq_cons: free the stats");
                free_attstatsslot(vardata->atttype, values, nvalues,
                                                  numbers, nnumbers);
        }
        else
        {
				elog(DEBUG4, "IND ADV: var_eq_cons: No ANALYZE stats available, so make a guess using estimated number.");
                /*
                 * No ANALYZE stats available, so make a guess using estimated number
                 * of distinct values and assuming they are equally common. (The guess
                 * is unlikely to be very good, but we do know a few special cases.)
                 */
                selec = 1.0 / get_variable_numdistinct(vardata, &isdefault);
        }

        /* result should be in range, but make sure... */
        CLAMP_PROBABILITY(selec);
		elog(DEBUG4, "IND ADV: var_eq_cons: END. retuning %.5f",selec);
        return selec;
}

/*
void dump_trace() {
	void * buffer[255];
	const int calls = backtrace(buffer,
		sizeof(buffer) / sizeof(void *));
	backtrace_symbols_fd(buffer, calls, 1);
}
*/


Configuration* parse_config_file(const char *filename, const char *cols, const bool text_pattern_ops, const int composit_max_cols, const bool read_only)
{
	// FILE* file;
	Configuration* config;
	// char line[256];
	// int linenum=0;
	// int params = 0;
	// char b[8];
	
	/*
	file = fopen(filename, "r");
	
    if (!file){
		elog(WARNING, "parse_config_file: file failed to open, %s",filename);
        return NULL;
	}
	*/
    
	config = palloc(sizeof(Configuration));
	config->text_pattern_ops = text_pattern_ops;
	config->composit_max_cols = composit_max_cols;
	strcpy(&(config->partQlauseCol),cols);
	config->read_only = read_only;
    
	/*
	while(fgets(line, 256, file) != NULL)
	{			
			linenum++;			
			if(line[0] == '#') continue;

			params = sscanf(line, "cols: %s", &(config->partQlauseCol));
			params = sscanf(line, "composit_max_cols: %d", &(config->composit_max_cols));
			if(!config->text_pattern_ops)
				config->text_pattern_ops = (sscanf(line,"text_pattern_ops: %[TtRrUuEe]",b)==1?true:false);			 
				
	}
	fclose(file);
	*/
	
    return config;
}


/*
   * get_opclass_name         - fetch name of an index operator class
   *
   * The opclass name is appended (after a space) to buf.
   *
   * Output is suppressed if the opclass is the default for the given
   * actual_datatype.  (If you don't want this behavior, just pass
   * InvalidOid for actual_datatype.)
   */
 void get_opclass_name(Oid opclass, Oid actual_datatype, StringInfo buf)
 {
	 HeapTuple   ht_opc;
	 Form_pg_opclass opcrec;
	 char       *opcname;
	 char       *nspname;
 
	 ht_opc = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));
	 if (!HeapTupleIsValid(ht_opc))
		elog(ERROR, "cache lookup failed for opclass %u", opclass);
	 opcrec = (Form_pg_opclass) GETSTRUCT(ht_opc);
 
	 if (!OidIsValid(actual_datatype) ||
	 GetDefaultOpClass(actual_datatype, opcrec->opcmethod) != opclass)
	 {
		 /* Okay, we need the opclass name.  Do we need to qualify it? */
		 opcname = NameStr(opcrec->opcname);
		 if (OpclassIsVisible(opclass))
			appendStringInfo(buf, " %s", quote_identifier(opcname));
		 else
		 {
			 nspname = get_namespace_name(opcrec->opcnamespace);
			 appendStringInfo(buf, " %s.%s",
			 quote_identifier(nspname),
			 quote_identifier(opcname));
		 }
	 }
	 ReleaseSysCache(ht_opc);
 }

List* create_operator_list(char *SupportedOps[]) 
{
	List*       opnos = NIL;
        int i;
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
	return opnos;
}
