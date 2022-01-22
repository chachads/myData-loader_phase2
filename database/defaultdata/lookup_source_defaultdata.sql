DELETE FROM lookup.lookup_source;
INSERT INTO lookup.lookup_source(internal_source_id, source_key, source_description, temp_table_definition, stage_table_name,source_format,source_date_format) VALUES (1, 'OPERA', 'OPERA reservation file.', 'stage.source_definition_opera', 'stage.stage_opera','CSV','MM/dd/yy');
INSERT INTO lookup.lookup_source(internal_source_id, source_key, source_description, temp_table_definition, stage_table_name,source_format,source_date_format) VALUES (2, 'ONQ_CRSSTAY', 'OnQ CRS stay data.', null, 'stage.stage_onq_crsstay','JSON','yyyy-MM-dd');
INSERT INTO lookup.lookup_source(internal_source_id, source_key, source_description, temp_table_definition, stage_table_name,source_format,source_date_format) VALUES (3, 'ONQ_PMSLEDGER', 'OnQ PMS ledger data', null, 'stage.stage_onq_pmsledger','JSON','yyyy-MM-dd');

