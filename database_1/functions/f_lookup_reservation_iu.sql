-- FUNCTION: lookup.f_lookup_reservation_iu(character varying)

DROP FUNCTION IF EXISTS lookup.f_lookup_reservation_iu(character varying,int);
CREATE SCHEMA IF NOT EXISTS lookup;

CREATE OR REPLACE FUNCTION lookup.f_lookup_reservation_iu(
	etlBatchId bigint,
	sourceId int)
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE internalReservationId integer;
BEGIN
	/*
	This routine works accepts a reservation key and source id. Upsert is tried and the internal reservation id is returned. Uniqueness of table f_lookup_reservation_iu is on reservationKey/source_id.
	*/
	CASE
	    WHEN sourceId = 1 THEN
	        INSERT INTO lookup.lookup_reservation (lookup_reservation_key,source_id)
            SELECT DISTINCT
            	s.reservation_id,
            	s.source_id
            FROM
            	stage.stage_opera s
            WHERE
                (etl_batch_id = etlBatchId OR etlBatchId IS NULL)
                AND s.source_id = sourceId
            ON CONFLICT (lookup_reservation_key,source_id)
            DO NOTHING;
	    WHEN sourceId = 2 THEN
	        INSERT INTO lookup.lookup_reservation (lookup_reservation_key,source_id)
            SELECT DISTINCT
            	concat(s.crs_inn_code,':',s.gnr,':',s.confirmation_number),
            	s.source_id
            FROM
            	stage.stage_onq_crsstay s
            WHERE
                (etl_batch_id = etlBatchId OR etlBatchId IS NULL)
                AND s.source_id = sourceId
            ON CONFLICT (lookup_reservation_key,source_id)
            DO NOTHING;
	    WHEN sourceId = 3 THEN
	        INSERT INTO lookup.lookup_reservation (lookup_reservation_key,source_id)
            SELECT DISTINCT
            	concat(s.crs_inn_code,':',s.gnr,':',s.confirmation_number),
            	s.source_id
            FROM
            	stage.stage_onq_crsstay s
            WHERE
                (etl_batch_id = etlBatchId OR etlBatchId IS NULL)
                AND s.source_id = sourceId
            ON CONFLICT (lookup_reservation_key,source_id)
            DO NOTHING;

	END CASE;
    return internalReservationId;
END;
$BODY$;
