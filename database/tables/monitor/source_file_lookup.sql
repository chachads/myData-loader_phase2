-- Table: main.source_file_lookup
CREATE SCHEMA IF NOT EXISTS monitor;

DROP TABLE IF EXISTS monitor.source_file_lookup CASCADE;

CREATE TABLE IF NOT EXISTS monitor.source_file_lookup
(
    source_file_lookup_id serial not null,
    source_id integer,
    raw_file_name  character varying COLLATE pg_catalog."default",
    bucket  character varying COLLATE pg_catalog."default",
    target_prefix character varying COLLATE pg_catalog."default",
    ingestion_status_id integer,
    record_timestamp timestamp without time zone DEFAULT current_timestamp,
    status_message character varying,
    CONSTRAINT source_file_lookup_pkey PRIMARY KEY (source_file_lookup_id)
)

TABLESPACE pg_default;

