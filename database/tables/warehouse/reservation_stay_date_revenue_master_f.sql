-- Table: warehouse.reservation_stay_date_revenue_master_f
CREATE SCHEMA IF NOT EXISTS warehouse;

DROP TABLE IF EXISTS warehouse.reservation_stay_date_revenue_master_f CASCADE;

CREATE TABLE IF NOT EXISTS warehouse.reservation_stay_date_revenue_master_f
(
    master_record_id bigserial not null,
    internal_stage_id bigint null,
    source_id int not null,
    internal_reservation_id bigint not null,
    internal_property_id bigint not null,
    business_date date,
    stay_date date,
    room_revenue decimal(38,10),
    food_revenue decimal(38,10),
    beverage_revenue decimal(38,10),
    phone_revenue decimal(38,10),
    shop_revenue  decimal(38,10),
    other_revenue decimal(38,10),
    non_revenue decimal(38,10),
    tax decimal(38,10),
    total_revenue decimal(38,10),
    etl_file_name character varying COLLATE pg_catalog."default",
    etl_ingest_datetime timestamp without time zone,
    CONSTRAINT reservation_stay_date_revenue_master_f_pk PRIMARY KEY (master_record_id,business_date)
)

PARTITION BY LIST (business_date);

ALTER TABLE IF EXISTS warehouse.reservation_stay_date_revenue_master_f
    OWNER to postgres;

COMMENT ON COLUMN warehouse.reservation_stay_date_revenue_master_f.internal_reservation_id IS 'MDO internal reservation id. Each new reservation is given a new internal id. Transfer from staging to reservation_f table creates the a new int_reservation_id';

