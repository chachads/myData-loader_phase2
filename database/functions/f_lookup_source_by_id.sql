CREATE SCHEMA IF NOT EXISTS lookup;
DROP FUNCTION IF EXISTS lookup.f_lookup_source_by_id(integer);
CREATE OR REPLACE FUNCTION lookup.f_lookup_source_by_id(
	sourceId integer)
    RETURNS character varying
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE sourceKey character varying;
BEGIN
    SELECT
        source_key INTO sourceKey
    FROM
        lookup.lookup_source
    WHERE
        internal_source_id = sourceId;
    RETURN sourceKey;
END;
$BODY$;
