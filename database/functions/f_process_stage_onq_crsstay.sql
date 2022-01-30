-- FUNCTION: lookup.f_process_stage_onq_crsstay(character varying)

DROP FUNCTION IF EXISTS lookup.f_process_stage_onq_crsstay(character varying);
CREATE SCHEMA IF NOT EXISTS lookup;

CREATE OR REPLACE FUNCTION lookup.f_process_stage_onq_crsstay(
	etlBatchId bigint)
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE rowCount integer;
DECLARE propertyCode character varying;
DECLARE sourceId integer;
DECLARE partitionValue character varying;
BEGIN
	/*
	This routine works only for 1 file and one property at a time.
	*/
	SELECT UPPER(crs_inn_code),source_id,CAST(partition_date AS character varying) INTO propertyCode,sourceId,partitionValue FROM  stage.stage_onq_crsstay WHERE (etl_batch_id = etlBatchId OR etlBatchId IS NULL);
	-- Create partition for business date if needed.
	PERFORM warehouse.f_create_table_partition('warehouse.reservation_stay_date_master_f',partitionValue);
	--PERFORM warehouse.f_create_table_partition('warehouse.reservation_business_date_f',partitionValue);
	--PERFORM warehouse.f_create_table_partition('warehouse.reservation_business_date_extension_f',partitionValue);
	PERFORM lookup.f_lookup_reservation_iu(etlBatchId,sourceId);

		INSERT INTO warehouse.reservation_stay_date_master_f
    	(
            source_id,
            internal_reservation_id,
            internal_property_id,
            business_date,
            stay_date,
            arrival_date,
            departure_date,
            rate_code,
            rate_code_description,
            room_rate,
            currency_code,
            room_type,
            etl_file_name,
            etl_ingest_datetime
        )
    	SELECT
            sourceId,
            lr.internal_reservation_id,
            lookup.f_lookup_property(propertyCode,sourceId),
            s.partition_date,
            s.stay_date,
            s.arrival_date,
            s.departure_date,
            s.srp_code,
            s.srp_name,
            s.prop_crs_room_rate,
            s.prop_currency_code,
            s.room_type_code,
            s.etl_file_name,
            current_timestamp
    	FROM
    		stage.stage_onq_crsstay s
			JOIN lookup.lookup_reservation lr ON lr.lookup_reservation_key =  concat(s.crs_inn_code,':',s.gnr,':',s.confirmation_number) AND lr.source_id = sourceId
    	WHERE
    		s.etl_batch_id = etlBatchId;

    DELETE FROM
        stage.stage_onq_crsstay
    WHERE
        etl_batch_id = etl_batch_id;

    return rowCount;
END
$BODY$;

--select * from lookup.f_process_stage_onq_crsstay('t815cae2910354e45a0b0b3a157c76c5e');

--select * from warehouse.reservation_business_date_extension_f;


select * from lookup.lookup_reservation limit 10;
