-- Table: warehouse.reservation_stay_date_f
CREATE SCHEMA IF NOT EXISTS warehouse;

DROP TABLE IF EXISTS warehouse.reservation_stay_date_f CASCADE;

CREATE TABLE IF NOT EXISTS warehouse.reservation_stay_date_f
(
    record_id serial not null,
    source_id int not null,
    internal_reservation_id bigint not null,
    internal_property_id bigint not null,
    business_date date,
    stay_date date,
    arrival_date date,
    check_in_datetime timestamp without time zone,
    departure_date date,
    check_out_datetime timestamp without time zone,
    rate_code character varying COLLATE pg_catalog."default",
    rate_code_description character varying COLLATE pg_catalog."default",
    room_rate decimal,
    currency_code character varying COLLATE pg_catalog."default",
    room_class character varying COLLATE pg_catalog."default",
    room_class_description character varying COLLATE pg_catalog."default",
    room_type character varying COLLATE pg_catalog."default",
    room_type_description character varying COLLATE pg_catalog."default",
    area_id character varying COLLATE pg_catalog."default",
    room_number character varying COLLATE pg_catalog."default",
    discount_amount decimal,
    discount_percentage decimal,
    discount_reason character varying COLLATE pg_catalog."default",
    room_revenue decimal,
    food_revenue decimal,
    other_revenue decimal,
    total_revenue decimal,
    non_revenue decimal,
    tax decimal,
    etl_file_name character varying COLLATE pg_catalog."default",
    etl_ingest_datetime timestamp without time zone,
    CONSTRAINT reservation_stay_date_f_pk PRIMARY KEY (record_id,business_date)
)
PARTITION BY LIST (business_date);

ALTER TABLE IF EXISTS warehouse.reservation_stay_date_f
    OWNER to postgres;

COMMENT ON COLUMN warehouse.reservation_stay_date_f.internal_reservation_id IS 'MDO internal reservation id. Each new reservation is given a new internal id. Transfer from staging to reservation_f table creates the a new int_reservation_id';

