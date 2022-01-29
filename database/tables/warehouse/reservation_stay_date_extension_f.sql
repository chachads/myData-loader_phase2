-- Table: warehouse.reservation_stay_date_extension_f
CREATE SCHEMA IF NOT EXISTS warehouse;

DROP TABLE IF EXISTS warehouse.reservation_stay_date_extension_f CASCADE;

CREATE TABLE IF NOT EXISTS warehouse.reservation_stay_date_extension_f
(
    record_id serial not null,
    source_id int not null,
    internal_reservation_id bigint not null,
    internal_property_id bigint not null,
    business_date date,
    confirmation_number character varying COLLATE pg_catalog."default",
    reservation_key character varying COLLATE pg_catalog."default",
    booking_id character varying COLLATE pg_catalog."default",
    guarantee_code character varying COLLATE pg_catalog."default",
    reservation_status character varying COLLATE pg_catalog."default",
    cancellation_date date,
    stay_rooms integer,
    stay_adults integer,
    stay_children integer,
    market_code character varying COLLATE pg_catalog."default",
    source_code character varying COLLATE pg_catalog."default",
    booked_room_class character varying COLLATE pg_catalog."default",
    booked_room_class_description character varying COLLATE pg_catalog."default",
    booked_room_type character varying COLLATE pg_catalog."default",
    booked_room_type_description character varying COLLATE pg_catalog."default",
    walkin_yn character varying COLLATE pg_catalog."default",
    complimentary_yn character varying COLLATE pg_catalog."default",
    house_use_yn character varying COLLATE pg_catalog."default",
    day_use_yn character varying COLLATE pg_catalog."default",
    vip_status character varying COLLATE pg_catalog."default",
    source_insert_date character varying COLLATE pg_catalog."default",
    source_update_date timestamp without time zone,
    booking_type character varying COLLATE pg_catalog."default",
    block_status character varying COLLATE pg_catalog."default",
    cancellation_code character varying COLLATE pg_catalog."default",
    channel character varying COLLATE pg_catalog."default",
    channel_type character varying COLLATE pg_catalog."default",
    etl_file_name character varying COLLATE pg_catalog."default",
    etl_ingest_datetime timestamp without time zone,
    CONSTRAINT reservation_stay_date_extension_f_pk PRIMARY KEY (record_id,business_date)
)
PARTITION BY LIST (business_date);

ALTER TABLE IF EXISTS warehouse.reservation_stay_date_extension_f
    OWNER to postgres;

COMMENT ON COLUMN warehouse.reservation_stay_date_extension_f.internal_reservation_id IS 'MDO internal reservation id. Each new reservation is given a new internal id. Transfer from staging to reservation_f table creates the a new int_reservation_id';

