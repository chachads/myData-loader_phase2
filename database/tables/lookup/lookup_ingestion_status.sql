CREATE SCHEMA IF NOT EXISTS lookup;
DROP TABLE IF EXISTS lookup.lookup_ingestion_status CASCADE;

    CREATE TABLE IF NOT EXISTS lookup.lookup_ingestion_status
(
    internal_status_id SERIAL NOT NULL,
    code character varying NOT NULL,
    status_description character varying,
    CONSTRAINT lookup_ingestion_status_pkey PRIMARY KEY (internal_status_id)
)

TABLESPACE pg_default;
