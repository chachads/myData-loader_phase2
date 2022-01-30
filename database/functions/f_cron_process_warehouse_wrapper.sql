-- FUNCTION: lookup.f_cron_process_warehouse_wrapper(character varying)

DROP FUNCTION IF EXISTS lookup.f_cron_process_warehouse_wrapper();
CREATE SCHEMA IF NOT EXISTS lookup;

CREATE OR REPLACE FUNCTION lookup.f_cron_process_warehouse_wrapper()
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE rowCount integer;
DECLARE propertyCode character varying;
DECLARE sourceId integer;
DECLARE partitionValue character varying;
DECLARE partitionCount integer;
DECLARE iPartition integer;
DECLARE p record;
BEGIN
    PERFORM lookup.f_process_warehouse_opera(null);
    PERFORM lookup.f_process_warehouse_onq_crsstay(null);
    return null;
END
$BODY$;


