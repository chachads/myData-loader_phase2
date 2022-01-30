-- FUNCTION: lookup.f_cron_process_warehouse_onq_crsstay(character varying)

DROP FUNCTION IF EXISTS lookup.f_cron_process_warehouse_onq_crsstay(bigint);
CREATE SCHEMA IF NOT EXISTS lookup;

CREATE OR REPLACE FUNCTION lookup.f_cron_process_warehouse_onq_crsstay(etlBatchId bigint)
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE sourceId integer;
DECLARE partitionRecord record;

BEGIN
    SELECT
        internal_source_id INTO sourceId
    FROM
        lookup.lookup_source
    WHERE
        source_key = 'ONQ_CRSSTAY';
    -- temp work table for this function. Gather all batches where source id matches onq crs stay, stage is completed and warehouse is not completed and warehouse timestamp is null.
	DROP TABLE IF EXISTS t_stage_to_warehouse_onq_crsstay;
	CREATE TEMPORARY TABLE t_stage_to_warehouse_onq_crsstay AS
	SELECT
		o.*,
		CAST(0 AS bigint) master_id,
		CONCAT(confirmation_number,':',gnr,':',crs_inn_code) reservation_key,
		-1 internal_reservation_id,
		-1 internal_property_id
	FROM
		stage.stage_batch b
		JOIN stage.stage_onq_crsstay o ON o.etl_batch_id = b.batch_id
	WHERE
		b.source_id = sourceId
		AND b.stage_completed_ind = true
		AND b.warehouse_completed_ind = false
		AND b.warehouse_transfer_start_timestamp IS NULL
		AND (b.etl_batch_id = etlBatchId OR etlBatchId IS NULL);
    -- mark these records as picked up so that another run does not pick it.
	UPDATE
		stage.stage_batch b
	SET
		warehouse_transfer_start_timestamp = current_timestamp
	FROM
		t_stage_to_warehouse_onq_crsstay o
	WHERE
		o.etl_batch_id = b.batch_id;
    -- START - GET internal reservation id and update in temp table.
	INSERT INTO lookup.lookup_reservation (lookup_reservation_key,source_id)
	SELECT DISTINCT
		s.reservation_key,
		s.source_id
	FROM
		t_stage_to_warehouse_onq_crsstay s
	ON CONFLICT (lookup_reservation_key,source_id)
	DO NOTHING;
	UPDATE
		t_stage_to_warehouse_onq_crsstay t
	SET
		internal_reservation_id = l.internal_reservation_id
	FROM
		lookup.lookup_reservation l
	WHERE
		l.lookup_reservation_key = t.reservation_key
		AND l.source_id = t.source_id;
    -- END - GET internal reservation id and update in temp table.

    -- START - GET internal property id and update in temp table.
	INSERT INTO lookup.lookup_property (property_code,source_id)
	SELECT DISTINCT
		UPPER(crs_inn_code),
		source_id
	FROM
		t_stage_to_warehouse_onq_crsstay
	ON CONFLICT (property_code,source_id)
	DO NOTHING;

	UPDATE
		t_stage_to_warehouse_onq_crsstay t
	SET
		internal_property_id = l.internal_property_id
	FROM
		lookup.lookup_property l
	WHERE
		UPPER(l.property_code) = UPPER(t.crs_inn_code)
		AND l.source_id = t.source_id;
    -- START - GET internal property id and update in temp table.
    -- START - Create partitions for all unique business date where business date is not null.
    FOR partitionRecord IN SELECT DISTINCT partition_date FROM t_stage_to_warehouse_onq_crsstay WHERE partition_date IS NOT NULL
        loop
            PERFORM warehouse.f_create_table_partition('warehouse.reservation_stay_date_master_f',CAST(partitionRecord.partition_date AS character varying));
            PERFORM warehouse.f_create_table_partition('warehouse.reservation_stay_date_extension_f',CAST(partitionRecord.partition_date AS character varying));
        end loop;
    -- END - Create partitions for all unique business date where business date is not null.
    INSERT INTO warehouse.reservation_stay_date_master_f
    (
        internal_stage_id,
        source_id,
        internal_reservation_id,
        internal_property_id,
        business_date,
        stay_date,
        arrival_date,
        departure_date,
        confirmation_number,
        room_rate,
        currency_code,
        guarantee_code,
        cancellation_number,
        market_category_code,
        number_of_adults,
        number_of_children,
        reservation_status,
        room_type_code,
        rate_plan_code,
        number_of_rooms,
        no_show_ind ,
        etl_file_name,
        etl_ingest_datetime
    )
    SELECT
        o.internal_stage_id,
        o.source_id,
        o.internal_reservation_id,
        o.internal_property_id,
        o.partition_date,
        o.stay_date,
        o.arrival_date,
        o.departure_date,
        o.confirmation_number,
        o.prop_crs_room_rate,
        o.prop_currency_code,
        o.guarantee_type_code,
        o.cancellation_number,
        o.mcat_code,
        o.number_of_adults,
        o.number_of_children,
        o.reservation_status,
        o.room_type_code,
        o.srp_code,
        1,
        o.no_show_ind,
        o.etl_file_name,
        current_timestamp
    FROM
        t_stage_to_warehouse_onq_crsstay o;


    UPDATE
        t_stage_to_warehouse_onq_crsstay t
    SET
        master_id = w.master_record_id
    FROM
        warehouse.reservation_stay_date_master_f w WHERE w.internal_stage_id = t.internal_stage_id AND w.business_date = t.partition_date;
    INSERT INTO warehouse.reservation_stay_date_extension_f
        (
            master_record_id,
            source_id,
            internal_reservation_id,
            internal_property_id,
            business_date,
            etl_file_name,
            reservation_extension_data
        )
        SELECT
            master_id,
            source_id,
            internal_reservation_id,
            internal_property_id,
            partition_date,
            etl_file_name,
            row_to_json(t)
      FROM (
            SELECT
                master_id,
                source_id,
                internal_reservation_id,
                internal_property_id,
                partition_date,
                airline_code,
                booked_date,
                booked_datetime,
                booking_segment_id,
                booking_segment_number,
                client_type,
                corporate_client_account_id,
                corporate_client_account_name,
                corporate_client_type_id,
                crs_inn_code,
                discount_rate_level,
                facility_id,
                filename,
                gnr,
                guarantee_type_text,
                honors_tier,
                iata_code,
                inncode,
                insert_datetime_utc,
                old_transaction_datetime_utc,
                originating_airline,
                originating_reservation_center,
                owner_account_id,
                owner_account_name,
                owner_extract_type,
                partition_by_date_id,
                srp_name,
                srp_type,
                tax_calculation_type,
                tax_included_ind,
                transaction_datetime_utc,
                version,
                etl_file_name
      FROM t_stage_to_warehouse_onq_crsstay
       ) t;
    UPDATE
        stage.stage_batch b
    SET
        warehouse_completed_ind = true,
        warehouse_transfer_completed_timestamp = current_timestamp
    FROM
            t_stage_to_warehouse_onq_crsstay o
        WHERE
            o.etl_batch_id = b.batch_id;
    -- END - insert into warehouse tables.
    -- START CLEAN UP
    DROP TABLE IF EXISTS t_distinct_etl_batch_ids;
    CREATE TEMPORARY TABLE
        t_distinct_etl_batch_ids AS
    SELECT
        DISTINCT etl_batch_id
    FROM
        t_stage_to_warehouse_onq_crsstay;
                     
    DELETE FROM
        stage.stage_onq_crsstay s
    USING
       t_distinct_etl_batch_ids t
    WHERE
        s.etl_batch_id = t.etl_batch_id;

    return null;
END
$BODY$;
