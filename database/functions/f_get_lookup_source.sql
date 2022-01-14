-- FUNCTION: lookup.f_get_lookup_source(character varying)

-- DROP FUNCTION IF EXISTS lookup.f_get_lookup_source(character varying);
CREATE SCHEMA IF NOT EXISTS lookup;

CREATE OR REPLACE FUNCTION lookup.f_get_lookup_source(
	sourcekey character varying)
    RETURNS TABLE(internal_source_id integer, source_key character varying, source_description character varying, temp_table_definition character varying, stage_table_name character varying, source_format character varying)
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
BEGIN
	return query
	SELECT
		d.internal_source_id,
		d.source_key,
		d.source_description,
		d.temp_table_definition,
		d.stage_table_name,
		d.source_format
	FROM
		lookup.lookup_source d
	WHERE
		d.source_key = sourceKey;
END;
$BODY$;
