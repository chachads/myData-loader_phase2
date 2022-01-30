-- FUNCTION: lookup.f_process_stage_onq_pmsledger(character varying)

DROP FUNCTION IF EXISTS lookup.f_process_stage_onq_pmsledger(character varying);
CREATE SCHEMA IF NOT EXISTS lookup;

CREATE OR REPLACE FUNCTION lookup.f_process_stage_onq_pmsledger(
	etlBatchId character varying)
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE rowCount integer;
BEGIN
    return rowCount;
END
$BODY$;

--select * from lookup.f_process_stage_onq_pmsledger('tc17ffa42dd584828abf25bfbe5f740e2');

--select * from warehouse.reservation_business_date_extension_f;


