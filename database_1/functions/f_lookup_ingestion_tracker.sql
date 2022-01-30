CREATE SCHEMA IF NOT EXISTS lookup;
DROP FUNCTION IF EXISTS lookup.f_lookup_ingestion_tracker(character varying,character varying);
CREATE OR REPLACE FUNCTION lookup.f_lookup_ingestion_tracker(
	sourceKey character varying,rawFileName character varying)
    RETURNS bigint
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE sourceTrackerId character varying;
BEGIN
    SELECT
        ingestion_tracker_id INTO sourceTrackerId
    FROM
        monitor.ingestion_tracker
    WHERE
        UPPER(source_key) = UPPER(sourceKey)
        AND UPPER(raw_file_name) = UPPER(rawFileName);
    RETURN sourceTrackerId;
END;
$BODY$;
