-- FUNCTION: lookup.f_lookup_property(character varying)

DROP FUNCTION IF EXISTS lookup.f_lookup_property(character varying,integer);
CREATE SCHEMA IF NOT EXISTS lookup;

CREATE OR REPLACE FUNCTION lookup.f_lookup_property(
	propertyCode character varying,sourceId integer)
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE internalPropertyId integer;
BEGIN
	/*
	This routine works accepts a property code and source id. Upsert is tried and the internal property id is returned. Uniqueness of table is on property_code/source_id.
	*/
	INSERT INTO lookup.lookup_property (property_code,source_id)
	VALUES(UPPER(propertyCode),sourceId)
	ON CONFLICT (property_code,source_id)
	DO NOTHING;

	SELECT internal_property_id INTO internalPropertyId FROM lookup.lookup_property WHERE UPPER(property_code) = UPPER(propertyCode) AND source_id = sourceId;

    return internalPropertyId;
END;
$BODY$;
