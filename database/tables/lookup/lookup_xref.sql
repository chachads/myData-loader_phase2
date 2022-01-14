CREATE SCHEMA IF NOT EXISTS lookup;
DROP TABLE IF EXISTS lookup.lookup_xref CASCADE;

CREATE TABLE IF NOT EXISTS lookup.lookup_xref
(
    internal_reference_id SERIAL,
    source_id integer NOT NULL,
    lookup_key_id integer NOT NULL,
    property_id integer NOT NULL,
    CONSTRAINT lookup_xref_pkey PRIMARY KEY (internal_reference_id)
)

TABLESPACE pg_default;
