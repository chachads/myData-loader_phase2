-- FUNCTION: lookup.f_cron_process_warehouse_opera(character varying)

DROP FUNCTION IF EXISTS lookup.f_cron_process_warehouse_opera();
CREATE SCHEMA IF NOT EXISTS lookup;

CREATE OR REPLACE FUNCTION lookup.f_cron_process_warehouse_opera()
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
		-1 internal_reservation_id,
		-1 internal_property_id
	FROM
		stage.stage_batch b
		JOIN stage.stage_opera o ON o.etl_batch_id = b.batch_id
	WHERE
		b.source_id = sourceId
		AND b.stage_completed_ind = true
		AND b.warehouse_completed_ind = false
		AND b.warehouse_transfer_start_timestamp IS NULL;
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
            PERFORM warehouse.f_create_table_partition('warehouse.reservation_stay_date_f',CAST(partitionRecord.business_date AS character varying));
            PERFORM warehouse.f_create_table_partition('warehouse.reservation_business_date_f',CAST(partitionRecord.business_date AS character varying));
            PERFORM warehouse.f_create_table_partition('warehouse.reservation_business_date_extension_f',CAST(partitionRecord.business_date AS character varying));
        end loop;
    -- END - Create partitions for all unique business date where business date is not null.
    -- START - insert into warehouse tables.
	INSERT INTO warehouse.reservation_stay_date_f
	(
        source_id,
        internal_reservation_id,
        internal_property_id,
        business_date,
        stay_date,
        arrival_date,
        check_in_datetime,
        departure_date,
        check_out_datetime,
        rate_code,
        rate_code_description,
        room_rate,
        currency_code,
        room_class,
        room_class_description,
        room_type,
        room_type_description,
        area_id,
        room_number,
        discount_amount,
        discount_percentage,
        discount_reason,
        room_revenue,
        food_revenue,
        other_revenue,
        total_revenue,
        non_revenue,
        tax,
        etl_file_name,
        etl_ingest_datetime
    )
	SELECT
        source_id,
        internal_reservation_id,
        s.internal_property_id,
        s.business_date,
        s.stay_date,
        s.begin_date,
        s.actual_check_in_date,
        s.end_date,
        s.actual_check_out_date,
        s.rate_code,
        s.rate_code_description,
        s.room_rate,
        s.currency,
        s.room_class,
        s.room_class_description,
        s.room_type,
        s.room_type_description,
        s.area_id,
        s.room_number,
        s.discount_amt,
        s.discount_prcnt,
        s.discount_reason,
        s.room_revenue,
        s.food_revenue,
        s.other_revenue,
        s.total_revenue,
        s.non_revenue,
        s.tax,
        s.etl_file_name,
        current_timestamp
	FROM
		t_stage_to_warehouse_opera s;

    INSERT INTO warehouse.reservation_business_date_f
    (
        source_id,
        internal_reservation_id,
        internal_property_id,
        business_date,
        confirmation_number,
        reservation_key,
        booking_id,
        guarantee_code,
        reservation_status,
        cancellation_date,
        stay_rooms,
        stay_adults,
        stay_children,
        market_code,
        source_code,
        booked_room_class,
        booked_room_class_description,
        booked_room_type,
        booked_room_type_description,
        walkin_yn,
        complimentary_yn,
        house_use_yn,
        day_use_yn,
        vip_status,
        booking_type,
        block_status,
        cancellation_code,
        channel,
        channel_type,
        source_insert_date,
        source_update_date,
        etl_file_name,
        etl_ingest_datetime
    )
    SELECT DISTINCT
		source_id,
		internal_reservation_id,
		internal_property_id,
        s.business_date,
        s.confirmation_no,
        s.reservation_id,
        s.booking_id,
        s.guarantee_code,
        s.resv_status,
        s.cancellation_date,
        s.stay_rooms,
        s.stay_adults,
        s.stay_children,
        s.market_code,
        s.source_code,
        s.booked_room_class,
        s.booked_room_class_description,
        s.booked_room_type,
        s.booked_room_type_description,
        s.walkin_yn,
        s.complimentary_yn,
        s.house_use_yn,
        s.day_use_yn,
        s.vip_status,
        s.booking_type,
        s.block_status,
        s.cancellation_code,
        s.channel,
        s.channel_type,
        s.insert_date,
        s.update_date,
        s.etl_file_name,
        current_timestamp
    FROM
       t_stage_to_warehouse_opera s;

    DROP TABLE IF EXISTS t_stage_opera_distinct;
    CREATE TEMPORARY TABLE t_stage_opera_distinct AS
	SELECT DISTINCT
	    etl_batch_id,
		source_id,
		internal_reservation_id,
		internal_property_id,
		reservation_id,
        business_date,
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
        s.etl_file_name
    FROM
        t_stage_to_warehouse_opera s;
    INSERT INTO warehouse.reservation_business_date_extension_f
    (
    source_id,
    internal_reservation_id,
    internal_property_id,
    business_date,
    reservation_extension_data,
    etl_file_name,
    etl_ingest_datetime
	)
	SELECT
		s.source_id,
		internal_reservation_id,
		internal_property_id,
        s.business_date,
        (
          SELECT row_to_json(d)
          FROM (
            SELECT
                vip_status
                ,guest_city
                ,guest_country
                ,guest_nationality
                ,membership_id
                ,membership_type
                ,membership_level
                ,membership_class
                ,travel_agent_id
                ,travel_agent_name
                ,travel_agent_address_line_1
                ,travel_agent_city
                ,travel_agent_state
                ,travel_agent_country
                ,travel_agent_postal_code
                ,company_id
                ,company_name
                ,company_address_line_1
                ,company_city
                ,company_state
                ,company_country
                ,company_postal_code
                ,allotment_header_id
                ,resv_name_id
            FROM t_stage_opera_distinct d
            WHERE d.reservation_id=s.reservation_id AND d.etl_batch_id = s.etl_batch_id
          ) d
        ),
        s.etl_file_name,
        current_timestamp
    FROM
        t_stage_opera_distinct s;
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
    DROP TABLE IF EXISTS t_stage_opera_distinct;
    return null;
END
$BODY$;

--select * from lookup.f_cron_process_warehouse_opera('tc17ffa42dd584828abf25bfbe5f740e2');

--select * from warehouse.reservation_business_date_extension_f;


