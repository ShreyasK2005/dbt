-- Call base ODS macro
-- Table mode = drop and rebuild table each run

{{ ods_base_table('erp','part', 'p_partkey') }}