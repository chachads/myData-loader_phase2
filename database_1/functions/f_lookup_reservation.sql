-- FUNCTION: lookup.f_lookup_reservation(character varying)

DROP FUNCTION IF EXISTS lookup.f_lookup_reservation(character varying,integer);
CREATE SCHEMA IF NOT EXISTS lookup;

CREATE OR REPLACE FUNCTION lookup.f_lookup_reservation(
	reservationKey character varying,sourceId integer)
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE internalReservationId integer;
BEGIN
	/*
	This routine works accepts a reservation key and source id. Upsert is tried and the internal reservation id is returned. Uniqueness of table f_lookup_reservation is on reservationKey/source_id.
	*/
	INSERT INTO lookup.lookup_reservation (lookup_reservation_key,source_id)
	VALUES(UPPER(reservationKey),sourceId)
	ON CONFLICT (lookup_reservation_key,source_id)
	DO NOTHING;

	SELECT internal_reservation_id INTO internalReservationId FROM lookup.lookup_reservation WHERE UPPER(lookup_reservation_key) = UPPER(reservationKey) AND source_id = sourceId;

    return internalReservationId;
END;
$BODY$;
