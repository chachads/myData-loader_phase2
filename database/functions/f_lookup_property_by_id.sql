-- FUNCTION: lookup.f_lookup_property_by_id(character varying)

DROP FUNCTION IF EXISTS lookup.f_lookup_property_by_id(bigint);
CREATE SCHEMA IF NOT EXISTS lookup;

CREATE OR REPLACE FUNCTION lookup.f_lookup_property_by_id(
	internalPropertyId bigint)
    RETURNS character varying
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE propertyCode character varying;
BEGIN
	SELECT
	    property_code INTO propertyCode
	FROM
	    lookup.lookup_property
	WHERE
	     internal_property_id = internal_property_id;

    return propertyCode;
END;
$BODY$;
