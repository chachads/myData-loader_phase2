CREATE SCHEMA IF NOT EXISTS lookup;
DROP TABLE IF EXISTS lookup.lookup_source CASCADE;

CREATE TABLE IF NOT EXISTS lookup.lookup_source
(
    internal_source_id integer NOT NULL,
    source_key character varying COLLATE pg_catalog."default",
    source_description character varying COLLATE pg_catalog."default",
    temp_table_definition character varying COLLATE pg_catalog."default",
    stage_table_name character varying COLLATE pg_catalog."default",
    source_format character varying COLLATE pg_catalog."default",
    CONSTRAINT lookup_source_pkey PRIMARY KEY (internal_source_id)
)

TABLESPACE pg_default;
