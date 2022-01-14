-- Table: warehouse.reservation_business_date_extension_f
CREATE SCHEMA IF NOT EXISTS warehouse;

DROP TABLE IF EXISTS warehouse.reservation_business_date_extension_f;

CREATE TABLE IF NOT EXISTS warehouse.reservation_business_date_extension_f
(
    record_id serial not null,
    source_id int not null,
    internal_reservation_id bigint not null,
    internal_property_id bigint not null,
    business_date date,
    reservation_extension_data JSONB,
    etl_file_name character varying COLLATE pg_catalog."default",
    etl_ingest_datetime timestamp without time zone,
    CONSTRAINT reservation_business_date_extension_f_pk PRIMARY KEY (record_id,business_date)
)
PARTITION BY LIST (business_date);

ALTER TABLE IF EXISTS warehouse.reservation_business_date_extension_f
    OWNER to postgres;

COMMENT ON COLUMN warehouse.reservation_business_date_extension_f.internal_reservation_id IS 'MDO internal reservation id. Each new reservation is given a new internal id. Transfer from staging to reservation_f table creates the a new int_reservation_id';

