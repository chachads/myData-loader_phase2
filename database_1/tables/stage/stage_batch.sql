CREATE SCHEMA IF NOT EXISTS stage;

DROP TABLE IF EXISTS stage.stage_batch CASCADE;

CREATE TABLE IF NOT EXISTS stage.stage_batch
(
    batch_id bigserial not null,
    source_id integer,
    etl_file_name character varying COLLATE pg_catalog."default",
    stage_completed_ind boolean default false,
    etl_start_timestamp timestamp without time zone DEFAULT current_timestamp,
    stage_completed_timestamp timestamp without time zone null,
    warehouse_transfer_start_timestamp timestamp without time zone null,
    warehouse_completed_ind boolean default false,
    warehouse_transfer_completed_timestamp  timestamp without time zone null,
    CONSTRAINT stage_batch_pk  PRIMARY KEY (batch_id)
)

TABLESPACE pg_default;
