CREATE SCHEMA IF NOT EXISTS stage;

DROP TABLE IF EXISTS stage.stage_opera CASCADE;

CREATE TABLE IF NOT EXISTS stage.stage_opera
(
    resort character varying COLLATE pg_catalog."default",
    business_date date,
    reservation_marker character varying COLLATE pg_catalog."default",
    confirmation_no character varying COLLATE pg_catalog."default",
    reservation_id character varying COLLATE pg_catalog."default",
    booking_id character varying COLLATE pg_catalog."default",
    guarantee_code character varying COLLATE pg_catalog."default",
    resv_status character varying COLLATE pg_catalog."default",
    cancellation_date date,
    stay_date date,
    begin_date date,
    actual_check_in_date timestamp without time zone,
    end_date date,
    actual_check_out_date timestamp without time zone,
    stay_rooms integer,
    stay_adults integer,
    stay_children integer,
    rate_code character varying COLLATE pg_catalog."default",
    rate_code_description character varying COLLATE pg_catalog."default",
    room_rate decimal(38,10),
    currency character varying COLLATE pg_catalog."default",
    market_code character varying COLLATE pg_catalog."default",
    source_code character varying COLLATE pg_catalog."default",
    room_class character varying COLLATE pg_catalog."default",
    room_class_description character varying COLLATE pg_catalog."default",
    booked_room_class character varying COLLATE pg_catalog."default",
    booked_room_class_description character varying COLLATE pg_catalog."default",
    room_type character varying COLLATE pg_catalog."default",
    room_type_description character varying COLLATE pg_catalog."default",
    booked_room_type character varying COLLATE pg_catalog."default",
    booked_room_type_description character varying COLLATE pg_catalog."default",
    area_id character varying COLLATE pg_catalog."default",
    room_number character varying COLLATE pg_catalog."default",
    walkin_yn character varying COLLATE pg_catalog."default",
    complimentary_yn character varying COLLATE pg_catalog."default",
    house_use_yn character varying COLLATE pg_catalog."default",
    day_use_yn character varying COLLATE pg_catalog."default",
    discount_amt decimal(38,10),
    discount_prcnt decimal(38,10),
    discount_reason character varying COLLATE pg_catalog."default",
    room_revenue decimal(38,10),
    food_revenue decimal(38,10),
    other_revenue decimal(38,10),
    total_revenue decimal(38,10),
    non_revenue decimal(38,10),
    tax decimal(38,10),
    vip_status character varying COLLATE pg_catalog."default",
    guest_city character varying COLLATE pg_catalog."default",
    guest_country character varying COLLATE pg_catalog."default",
    guest_nationality character varying COLLATE pg_catalog."default",
    membership_id character varying COLLATE pg_catalog."default",
    membership_type character varying COLLATE pg_catalog."default",
    membership_level character varying COLLATE pg_catalog."default",
    membership_class character varying COLLATE pg_catalog."default",
    travel_agent_id character varying COLLATE pg_catalog."default",
    travel_agent_name character varying COLLATE pg_catalog."default",
    travel_agent_address_line_1 character varying COLLATE pg_catalog."default",
    travel_agent_city character varying COLLATE pg_catalog."default",
    travel_agent_state character varying COLLATE pg_catalog."default",
    travel_agent_country character varying COLLATE pg_catalog."default",
    travel_agent_postal_code character varying COLLATE pg_catalog."default",
    company_id character varying COLLATE pg_catalog."default",
    company_name character varying COLLATE pg_catalog."default",
    company_address_line_1 character varying COLLATE pg_catalog."default",
    company_city character varying COLLATE pg_catalog."default",
    company_state character varying COLLATE pg_catalog."default",
    company_country character varying COLLATE pg_catalog."default",
    company_postal_code character varying COLLATE pg_catalog."default",
    allotment_header_id character varying COLLATE pg_catalog."default",
    resv_name_id character varying COLLATE pg_catalog."default",
    booking_type character varying COLLATE pg_catalog."default",
    block_status character varying COLLATE pg_catalog."default",
    cancellation_code character varying COLLATE pg_catalog."default",
    channel character varying COLLATE pg_catalog."default",
    channel_type character varying COLLATE pg_catalog."default",
    insert_date character varying COLLATE pg_catalog."default",
    update_date timestamp without time zone,
    etl_batch_id bigint,
    source_id integer,
    etl_file_name character varying COLLATE pg_catalog."default",
    etl_ingest_datetime timestamp without time zone,
    internal_stage_id bigserial not null
)

TABLESPACE pg_default;
