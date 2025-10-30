-- Call base ODS macro
-- Table mode = drop and rebuild table each run
{{ ods_base_table('erp','lineitem', ['l_orderkey','l_linenumber']) }}