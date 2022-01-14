CREATE SCHEMA IF NOT EXISTS lookup;
DROP TABLE IF EXISTS lookup.lookup_reservation CASCADE;

CREATE TABLE IF NOT EXISTS lookup.lookup_reservation
(
    internal_reservation_id SERIAL NOT NULL,
    lookup_reservation_key character varying NOT NULL,
    source_id INT NOT NULL,
    CONSTRAINT lookup_reservation_pkey PRIMARY KEY (internal_reservation_id),
    UNIQUE(lookup_reservation_key,source_id)
)

TABLESPACE pg_default;


COMMENT ON COLUMN lookup.lookup_reservation.lookup_reservation_key IS 'Value that uniquely identifies a reservation by source. This will be used across the warehouse in the fact tables and will be used in combination with source_id';