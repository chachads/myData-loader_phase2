-- Table: stage.stage_onq_crsstay
CREATE SCHEMA IF NOT EXISTS stage;

DROP TABLE IF EXISTS stage.stage_onq_crsstay CASCADE;

CREATE TABLE IF NOT EXISTS stage.stage_onq_crsstay
(
    etl_batch_id character varying COLLATE pg_catalog."default",
    confirmation_number character varying COLLATE pg_catalog."default",
    gnr character varying COLLATE pg_catalog."default",
    crs_inn_code character varying COLLATE pg_catalog."default",
    stay_date date,
    booked_date date,
    booked_datetime timestamp without time zone,
    arrival_date date,
    departure_date date,
    mcat_code character varying COLLATE pg_catalog."default",
    srp_code character varying COLLATE pg_catalog."default",
    srp_type character varying COLLATE pg_catalog."default",
    srp_name character varying COLLATE pg_catalog."default",
    room_type_code character varying COLLATE pg_catalog."default",
    honors_tier character varying COLLATE pg_catalog."default",
    prop_crs_room_rate decimal,
    prop_currency_code character varying COLLATE pg_catalog."default",
    tax_included_ind boolean,
    originating_reservation_center character varying COLLATE pg_catalog."default",
    originating_airline character varying COLLATE pg_catalog."default",
    airline_code character varying COLLATE pg_catalog."default",
    corporate_client_account_id  character varying COLLATE pg_catalog."default",
    corporate_client_account_name character varying COLLATE pg_catalog."default",
    corporate_client_type_id character varying COLLATE pg_catalog."default",
    client_type character varying COLLATE pg_catalog."default",
    iata_code  character varying COLLATE pg_catalog."default",
    number_of_adults integer,
    number_of_children integer,
    cancellation_number character varying COLLATE pg_catalog."default",
    guarantee_type_code character varying COLLATE pg_catalog."default",
    guarantee_type_text character varying COLLATE pg_catalog."default",
    discount_rate_level character varying COLLATE pg_catalog."default",
    no_show_ind boolean,
    tax_calculation_type character varying COLLATE pg_catalog."default",
    gnr_comment_subject character varying COLLATE pg_catalog."default",
    reservation_status character varying COLLATE pg_catalog."default",
    old_transaction_datetime_utc timestamp without time zone,
    booking_segment_number integer,
    transaction_datetime_utc timestamp without time zone,
    booking_segment_id integer,
    partition_by_date_id date,
    filename character varying COLLATE pg_catalog."default",
    etl_file_name character varying COLLATE pg_catalog."default",
    etl_ingest_datetime timestamp without time zone
)

TABLESPACE pg_default;
