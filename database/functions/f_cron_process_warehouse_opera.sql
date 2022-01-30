-- FUNCTION: lookup.f_cron_process_warehouse_opera(character varying)

DROP FUNCTION IF EXISTS lookup.f_cron_process_warehouse_opera(bigint);
CREATE SCHEMA IF NOT EXISTS lookup;

CREATE OR REPLACE FUNCTION lookup.f_cron_process_warehouse_opera(etlBatchId bigint)
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
        source_key = 'OPERA';
    -- temp work table for this function. Gather all batches where source id matches opera, stage is completed and warehouse is not completed and warehouse timestamp is null.
	DROP TABLE IF EXISTS t_stage_to_warehouse_opera;
	CREATE TEMPORARY TABLE t_stage_to_warehouse_opera AS
	SELECT
		o.*,
		CAST(0 AS bigint) master_id,
		-1 internal_reservation_id,
		-1 internal_property_id
	FROM
		stage.stage_batch b
		JOIN stage.stage_opera o ON o.etl_batch_id = b.batch_id
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
		t_stage_to_warehouse_opera o
	WHERE
		o.etl_batch_id = b.batch_id;
    -- START - GET internal reservation id and update in temp table.
	INSERT INTO lookup.lookup_reservation (lookup_reservation_key,source_id)
	SELECT DISTINCT
		s.reservation_id,
		s.source_id
	FROM
		t_stage_to_warehouse_opera s
	ON CONFLICT (lookup_reservation_key,source_id)
	DO NOTHING;
	UPDATE
		t_stage_to_warehouse_opera t
	SET
		internal_reservation_id = l.internal_reservation_id
	FROM
		lookup.lookup_reservation l
	WHERE
		l.lookup_reservation_key = t.reservation_id
		AND l.source_id = t.source_id;
    -- END - GET internal reservation id and update in temp table.

    -- START - GET internal property id and update in temp table.
	INSERT INTO lookup.lookup_property (property_code,source_id)
	SELECT DISTINCT
		UPPER(resort),
		source_id
	FROM
		t_stage_to_warehouse_opera
	ON CONFLICT (property_code,source_id)
	DO NOTHING;

	UPDATE
		t_stage_to_warehouse_opera t
	SET
		internal_property_id = l.internal_property_id
	FROM
		lookup.lookup_property l
	WHERE
		UPPER(l.property_code) = UPPER(t.resort)
		AND l.source_id = t.source_id;
    -- START - GET internal property id and update in temp table.
    -- START - Create partitions for all unique business date where business date is not null.
    FOR partitionRecord IN SELECT DISTINCT business_date FROM t_stage_to_warehouse_opera WHERE business_date IS NOT NULL
        loop
            PERFORM warehouse.f_create_table_partition('warehouse.reservation_stay_date_master_f',CAST(partitionRecord.business_date AS character varying));
            PERFORM warehouse.f_create_table_partition('warehouse.reservation_stay_date_extension_f',CAST(partitionRecord.business_date AS character varying));
        end loop;
    -- END - Create partitions for all unique business date where business date is not null.
    -- START - insert into warehouse tables.

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
        cancellation_date,
        channel_code,
        channel_type,
        market_category_code,
        number_of_adults,
        number_of_children,
        reservation_status,
        room_type_code,
        room_class_code,
        room_number,
        rate_plan_code,
        number_of_rooms,
        no_show_ind,
        walkin_ind,
        complimentary_ind,
        house_use_ind,
        day_user_ind,
        etl_file_name,
        etl_ingest_datetime
    )
    SELECT
        o.internal_stage_id,
        o.source_id,
        o.internal_reservation_id,
        o.internal_property_id,
        business_date,
        stay_date,
        begin_date,
        end_date,
        confirmation_no,
        room_rate,
        currency,
        guarantee_code,
        cancellation_code,
        cancellation_date,
        channel,
        channel_type,
        market_code,
        stay_adults,
        stay_children,
        resv_status,
        room_type,
        room_class,
        room_number,
        rate_code,
        stay_rooms,
        null,
        walkin_yn,
        complimentary_yn,
        house_use_yn,
        day_use_yn,
        o.etl_file_name,
        current_timestamp
    FROM
        t_stage_to_warehouse_opera o;

    UPDATE
        t_stage_to_warehouse_opera t
    SET
        master_id = w.master_record_id
    FROM
        warehouse.reservation_stay_date_master_f w WHERE w.internal_stage_id = t.internal_stage_id AND w.business_date = t.business_date;
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
            business_date,
            etl_file_name,
            row_to_json(t)
      FROM (
            SELECT
                master_id,
                source_id,
                internal_reservation_id,
                internal_property_id,
                business_date,
                resort,
                reservation_marker,
                reservation_id,
                booking_id,
                actual_check_in_date,
                actual_check_out_date,
                rate_code,
                rate_code_description,
                source_code,
                room_class,
                room_class_description,
                booked_room_class,
                booked_room_class_description,
                room_type_description,
                booked_room_type,
                booked_room_type_description,
                area_id,
                discount_amt,
                discount_prcnt,
                discount_reason,
                room_revenue,
                food_revenue,
                other_revenue,
                total_revenue,
                non_revenue,
                tax,
                vip_status,
                guest_city,
                guest_country,
                guest_nationality,
                membership_id,
                membership_type,
                membership_level,
                membership_class,
                travel_agent_id,
                travel_agent_name,
                travel_agent_address_line_1,
                travel_agent_city,
                travel_agent_state,
                travel_agent_country,
                travel_agent_postal_code,
                company_id,
                company_name,
                company_address_line_1,
                company_city,
                company_state,
                company_country,
                company_postal_code,
                allotment_header_id,
                resv_name_id,
                booking_type,
                block_status,
                insert_date,
                update_date,
                etl_file_name
      FROM t_stage_to_warehouse_opera
       ) t;
    -- Update status in stage.stage_batch
    UPDATE
        stage.stage_batch b
    SET
        warehouse_completed_ind = true,
        warehouse_transfer_completed_timestamp = current_timestamp
    FROM
            t_stage_to_warehouse_opera o
        WHERE
            o.etl_batch_id = b.batch_id;
    -- END - insert into warehouse tables.
    -- START CLEAN UP

    DELETE FROM
        stage.stage_opera s
    USING
       t_stage_to_warehouse_opera t
    WHERE
        s.etl_batch_id = t.etl_batch_id;
  	DROP TABLE IF EXISTS t_stage_to_warehouse_opera;
    return null;
END
$BODY$;


