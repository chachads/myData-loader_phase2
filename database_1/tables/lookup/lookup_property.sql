CREATE SCHEMA IF NOT EXISTS lookup;
DROP TABLE IF EXISTS lookup.lookup_property CASCADE;

    CREATE TABLE IF NOT EXISTS lookup.lookup_property
(
    internal_property_id SERIAL NOT NULL,
    property_code character varying NOT NULL,
    source_id INT NOT NULL,
    CONSTRAINT lookup_property_pkey PRIMARY KEY (internal_property_id),
    UNIQUE (property_code,source_id)
)

TABLESPACE pg_default;
