DELETE FROM lookup.lookup_source;
INSERT INTO lookup.lookup_source(internal_source_id, source_key, source_description, temp_table_definition, stage_table_name,source_format,source_date_format) VALUES (1, 'OPERA', 'OPERA', 'stage.source_definition_opera', 'stage.stage_opera','CSV','MM/dd/yy');
INSERT INTO lookup.lookup_source(internal_source_id, source_key, source_description, temp_table_definition, stage_table_name,source_format,source_date_format) VALUES (2, 'ONQ_CRSSTAY', 'On', 'stage.source_definition_opera', 'stage.stage_opera','JSON','yyyy-MMdd');
INSERT INTO lookup.lookup_source(internal_source_id, source_key, source_description, temp_table_definition, stage_table_name,source_format,source_date_format) VALUES (3, 'ONQ_PMSLEDGER', 'On', 'stage.source_definition_onq_pmsledgerjson', 'stage.stage_onq_pmsledger','JSON','yyyy-MM-dd');

