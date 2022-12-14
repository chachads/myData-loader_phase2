CREATE SCHEMA IF NOT EXISTS lookup;
DROP FUNCTION IF EXISTS lookup.f_lookup_source(character varying);
CREATE OR REPLACE FUNCTION lookup.f_lookup_source(
	sourcekey character varying)
    RETURNS TABLE(internal_source_id integer, source_key character varying, source_description character varying, temp_table_definition character varying, stage_table_name character varying, source_format character varying,source_date_format character varying,source_timestamp_format character varying,warehouse_function_name character varying)
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
		d.source_format,
		d.source_date_format,
		d.source_timestamp_format,
		d.warehouse_function_name
	FROM
		lookup.lookup_source d
	WHERE
		d.source_key = sourceKey;
END;
$BODY$;
