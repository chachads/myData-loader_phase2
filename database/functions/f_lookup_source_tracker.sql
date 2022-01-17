CREATE SCHEMA IF NOT EXISTS lookup;
DROP FUNCTION IF EXISTS lookup.f_lookup_source_tracker(character varying,character varying);
CREATE OR REPLACE FUNCTION lookup.f_lookup_source_tracker(
	sourceKey character varying,rawFileName character varying)
    RETURNS bigint
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE sourceTrackerId character varying;
BEGIN
    SELECT
        source_tracker_id INTO sourceTrackerId
    FROM
        monitor.source_tracker
    WHERE
        UPPER(source_key) = UPPER(sourceKey)
        AND UPPER(raw_file_name) = UPPER(rawFileName);
    RETURN sourceTrackerId;
END;
$BODY$;
