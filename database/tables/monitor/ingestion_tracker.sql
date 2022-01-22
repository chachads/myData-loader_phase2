-- Table: main.ingestion_tracker
CREATE SCHEMA IF NOT EXISTS monitor;

DROP TABLE IF EXISTS monitor.ingestion_tracker CASCADE;

CREATE TABLE IF NOT EXISTS monitor.ingestion_tracker
(
    ingestion_tracker_id serial not null,
    source_key character varying,
    source_type character varying,
    raw_file_name  character varying COLLATE pg_catalog."default",
    source_bucket character varying COLLATE pg_catalog."default",
    source_prefix character varying COLLATE pg_catalog."default",
    target_bucket  character varying COLLATE pg_catalog."default",
    target_prefix character varying COLLATE pg_catalog."default",
    stage_db_table character varying COLLATE pg_catalog."default",
    insert_row_count integer,
    lambda_event_trigger_time timestamp without time zone,
    stage_write_time timestamp without time zone,
    warehouse_write_time timestamp without time zone,
    rdz_write_time timestamp without time zone,
    ingestion_status_id integer,
    status_message character varying,
    CONSTRAINT file_tracker_pkey PRIMARY KEY (ingestion_tracker_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS monitor.ingestion_tracker
    OWNER to postgres;
