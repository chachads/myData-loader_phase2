DELETE FROM lookup.lookup_source;
INSERT INTO lookup.lookup_source(internal_source_id, source_key, source_description, temp_table_definition, stage_table_name,source_format) VALUES (1, 'OPERA', 'OPERA', 'stage.source_definition_opera', 'stage.stage_opera','CSV');
INSERT INTO lookup.lookup_source(internal_source_id, source_key, source_description, temp_table_definition, stage_table_name,source_format) VALUES (2, 'ONQ_CRSSTAY', 'On', 'stage.source_definition_opera', 'stage.stage_opera','JSON');
INSERT INTO lookup.lookup_source(internal_source_id, source_key, source_description, temp_table_definition, stage_table_name,source_format) VALUES (3, 'ONQ_PMSLEDGER', 'On', 'stage.source_definition_onq_pmsledgerjson', 'stage.stage_onq_pmsledger','JSON');

