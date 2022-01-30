-- Table: warehouse.reservation_stay_date_master_f
CREATE SCHEMA IF NOT EXISTS warehouse;

DROP TABLE IF EXISTS warehouse.reservation_stay_date_master_f CASCADE;

CREATE TABLE IF NOT EXISTS warehouse.reservation_stay_date_master_f
(
    master_record_id bigserial not null,
    internal_stage_id bigint null,
    source_id int not null,
    internal_reservation_id bigint not null,
    internal_property_id bigint not null,
    business_date date,
    stay_date date,
    arrival_date date,
    departure_date date,
    confirmation_number character varying COLLATE pg_catalog."default",
    room_rate decimal(38,10),
    currency_code character varying COLLATE pg_catalog."default",
    guarantee_code character varying COLLATE pg_catalog."default",
    cancellation_number character varying COLLATE pg_catalog."default",
    cancellation_date date,
    channel_code character varying COLLATE pg_catalog."default",
    channel_type character varying COLLATE pg_catalog."default",
    market_category_code character varying COLLATE pg_catalog."default",
    number_of_adults integer,
    number_of_children integer,
    reservation_status character varying COLLATE pg_catalog."default",
    room_type_code character varying COLLATE pg_catalog."default",
    room_class_code character varying COLLATE pg_catalog."default",
    room_number character varying COLLATE pg_catalog."default",
    rate_plan_code character varying COLLATE pg_catalog."default",
    number_of_rooms integer,
    no_show_ind  character varying COLLATE pg_catalog."default",
    walkin_ind character varying COLLATE pg_catalog."default",
    complimentary_ind character varying COLLATE pg_catalog."default",
    house_use_ind character varying COLLATE pg_catalog."default",
    day_user_ind character varying COLLATE pg_catalog."default",
    etl_file_name character varying COLLATE pg_catalog."default",
    etl_ingest_datetime timestamp without time zone,
    CONSTRAINT reservation_stay_date_master_f_pk PRIMARY KEY (master_record_id,business_date)
)



PARTITION BY LIST (business_date);

ALTER TABLE IF EXISTS warehouse.reservation_stay_date_master_f
    OWNER to postgres;

COMMENT ON COLUMN warehouse.reservation_stay_date_master_f.internal_reservation_id IS 'MDO internal reservation id. Each new reservation is given a new internal id. Transfer from staging to reservation_f table creates the a new int_reservation_id';

