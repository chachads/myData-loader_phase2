-- Table: stage.stage_onq_crsstay
CREATE SCHEMA IF NOT EXISTS stage;

DROP TABLE IF EXISTS stage.stage_onq_crsstay CASCADE;

CREATE TABLE IF NOT EXISTS stage.stage_onq_crsstay
(
    etl_batch_id bigint,
    airline_code character varying COLLATE pg_catalog."default" null,
    arrival_date date null,
    booked_date date null,
    booked_datetime timestamp without time zone,
    booking_segment_id character varying COLLATE pg_catalog."default" null,
    booking_segment_number bigint null,
    cancellation_number character varying COLLATE pg_catalog."default" null,
    client_type character varying COLLATE pg_catalog."default" null,
    confirmation_number character varying COLLATE pg_catalog."default" null,
    corporate_client_account_id character varying COLLATE pg_catalog."default" null,
    corporate_client_account_name character varying COLLATE pg_catalog."default" null,
    corporate_client_type_id character varying COLLATE pg_catalog."default" null,
    crs_inn_code character varying COLLATE pg_catalog."default" null,
    departure_date date null,
    discount_rate_level character varying COLLATE pg_catalog."default" null,
    facility_id character varying COLLATE pg_catalog."default" null,
    filename character varying COLLATE pg_catalog."default" null,
    gnr character varying COLLATE pg_catalog."default" null,
    guarantee_type_code character varying COLLATE pg_catalog."default" null,
    guarantee_type_text character varying COLLATE pg_catalog."default" null,
    honors_tier character varying COLLATE pg_catalog."default" null,
    iata_code character varying COLLATE pg_catalog."default" null,
    inncode character varying COLLATE pg_catalog."default" null,
    insert_datetime_utc timestamp without time zone,
    mcat_code character varying COLLATE pg_catalog."default" null,
    no_show_ind boolean null,
    number_of_adults bigint null,
    number_of_children bigint null,
    old_transaction_datetime_utc timestamp without time zone,
    originating_airline character varying COLLATE pg_catalog."default" null,
    originating_reservation_center character varying COLLATE pg_catalog."default" null,
    owner_account_id character varying COLLATE pg_catalog."default" null,
    owner_account_name character varying COLLATE pg_catalog."default" null,
    owner_extract_type character varying COLLATE pg_catalog."default" null,
    partition_by_date_id character varying COLLATE pg_catalog."default" null,
    partition_date date null,
    prop_crs_room_rate decimal null,
    prop_currency_code character varying COLLATE pg_catalog."default" null,
    reservation_status character varying COLLATE pg_catalog."default" null,
    room_type_code character varying COLLATE pg_catalog."default" null,
    srp_code character varying COLLATE pg_catalog."default" null,
    srp_name character varying COLLATE pg_catalog."default" null,
    srp_type character varying COLLATE pg_catalog."default" null,
    stay_date date null,
    tax_calculation_type character varying COLLATE pg_catalog."default" null,
    tax_included_ind character varying COLLATE pg_catalog."default" null,
    transaction_datetime_utc timestamp without time zone,
    version character varying COLLATE pg_catalog."default" null,
    source_id integer,
    etl_file_name character varying COLLATE pg_catalog."default" null,
    etl_ingest_datetime timestamp without time zone
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS stage.stage_onq_crsstay
    OWNER to postgres;

    /*


*/