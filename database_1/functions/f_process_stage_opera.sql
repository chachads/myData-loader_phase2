-- FUNCTION: lookup.f_process_stage_opera(character varying)

DROP FUNCTION IF EXISTS lookup.f_process_stage_opera(bigint);
CREATE SCHEMA IF NOT EXISTS lookup;

CREATE OR REPLACE FUNCTION lookup.f_process_stage_opera(
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
	SELECT UPPER(resort),source_id,CAST(business_date AS character varying) INTO propertyCode,sourceId,partitionValue FROM  stage.stage_opera WHERE (etl_batch_id = etlBatchId OR etlBatchId IS NULL);
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
        sourceId,
        lr.internal_reservation_id,
        lookup.f_lookup_property(propertyCode,sourceId),
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
		stage.stage_opera s
    	JOIN lookup.lookup_reservation lr ON lr.lookup_reservation_key =  s.reservation_id AND lr.source_id = s.source_Id
	WHERE
		(s.etl_batch_id = etlBatchId OR etlBatchId IS NULL)
		AND s.source_id = sourceId;


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
		sourceId,
		lookup.f_lookup_reservation(s.reservation_id,sourceId),
		lookup.f_lookup_property(propertyCode,sourceId),
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
        stage.stage_opera s
        JOIN lookup.lookup_reservation lr ON lr.lookup_reservation_key =  s.reservation_id AND lr.source_id = s.source_Id
    WHERE
		(s.etl_batch_id = etlBatchId OR etlBatchId IS NULL)
		AND s.source_id = sourceId;

    DELETE FROM
        stage.stage_opera_distinct
    WHERE
		(etl_batch_id = etlBatchId OR etlBatchId IS NULL)
		AND source_id = sourceId;

    INSERT INTO stage.stage_opera_distinct
    (
        etl_batch_id,
        source_id,
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
        etl_file_name    
    )
	SELECT DISTINCT
	    etl_batch_id,
		source_id,
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
        stage.stage_opera s
    WHERE
		(s.etl_batch_id = etlBatchId OR etlBatchId IS NULL)
		AND s.source_id = sourceId;





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
		lr.internal_reservation_id,
		lookup.f_lookup_property(propertyCode,s.source_id),
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
            FROM stage.stage_opera_distinct d
            WHERE d.reservation_id=s.reservation_id AND d.etl_batch_id = s.etl_batch_id
          ) d
        ),
        s.etl_file_name,
        current_timestamp
    FROM
        stage.stage_opera_distinct s
    	JOIN lookup.lookup_reservation lr ON lr.lookup_reservation_key =  s.reservation_id AND lr.source_id = s.source_Id
    WHERE
		(s.etl_batch_id = etlBatchId OR etlBatchId IS NULL)
		AND s.source_id = sourceId;

    DELETE FROM
        stage.stage_opera_distinct
    WHERE
		(etl_batch_id = etlBatchId OR etlBatchId IS NULL)
		AND source_id = sourceId;

/*
    DELETE FROM
        stage.stage_opera
    WHERE
        etl_batch_id = etlBatchId;
*/

    return rowCount;
END
$BODY$;

--select * from lookup.f_process_stage_opera('tc17ffa42dd584828abf25bfbe5f740e2');

--select * from warehouse.reservation_business_date_extension_f;


