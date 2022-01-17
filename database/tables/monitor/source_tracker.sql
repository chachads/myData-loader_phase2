-- Table: main.source_tracker
CREATE SCHEMA IF NOT EXISTS monitor;

DROP TABLE IF EXISTS monitor.source_tracker CASCADE;

CREATE TABLE IF NOT EXISTS monitor.source_tracker
(
    source_tracker_id serial not null,
    source_key character varying,
    source_type character varying,
    raw_file_name  character varying COLLATE pg_catalog."default",
    source_bucket character varying COLLATE pg_catalog."default",
    source_prefix character varying COLLATE pg_catalog."default",
    target_bucket  character varying COLLATE pg_catalog."default",
    target_prefix character varying COLLATE pg_catalog."default",
    target_db_table character varying COLLATE pg_catalog."default",
    insert_row_count integer,
    process_start_time timestamp without time zone,
    process_db_write_time timestamp without time zone,
    process_rdz_write_time timestamp without time zone,
    internal_ingestion_status integer,
    CONSTRAINT file_tracker_pkey PRIMARY KEY (source_tracker_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS monitor.source_tracker
    OWNER to postgres;
