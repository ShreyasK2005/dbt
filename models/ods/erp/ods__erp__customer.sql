-- Call base ODS macro
-- Table mode = drop and rebuild table each run

{{ ods_base_table('erp','customer', 'c_custkey') }}