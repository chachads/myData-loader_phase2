CREATE SCHEMA IF NOT EXISTS lookup;
DROP TABLE IF EXISTS lookup.lookup_key CASCADE;

CREATE TABLE IF NOT EXISTS lookup.lookup_key
(
    lookup_key_id integer NOT NULL,
    lookup_key character varying NOT NULL,
    CONSTRAINT lookup_key_pkey PRIMARY KEY (lookup_key_id)
)

TABLESPACE pg_default;
