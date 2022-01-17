CREATE SCHEMA IF NOT EXISTS stage;

DROP TABLE IF EXISTS stage.stage_opera_distinct CASCADE;

CREATE TABLE IF NOT EXISTS stage.stage_opera_distinct
(
    stage_id serial not null,
    etl_batch_id character varying COLLATE pg_catalog."default",
    source_id integer,
    reservation_id character varying COLLATE pg_catalog."default",
    business_date date,
    vip_status character varying COLLATE pg_catalog."default",
    guest_city character varying COLLATE pg_catalog."default",
    guest_country character varying COLLATE pg_catalog."default",
    guest_nationality character varying COLLATE pg_catalog."default",
    membership_id character varying COLLATE pg_catalog."default",
    membership_type character varying COLLATE pg_catalog."default",
    membership_level character varying COLLATE pg_catalog."default",
    membership_class character varying COLLATE pg_catalog."default",
    travel_agent_id character varying COLLATE pg_catalog."default",
    travel_agent_name character varying COLLATE pg_catalog."default",
    travel_agent_address_line_1 character varying COLLATE pg_catalog."default",
    travel_agent_city character varying COLLATE pg_catalog."default",
    travel_agent_state character varying COLLATE pg_catalog."default",
    travel_agent_country character varying COLLATE pg_catalog."default",
    travel_agent_postal_code character varying COLLATE pg_catalog."default",
    company_id character varying COLLATE pg_catalog."default",
    company_name character varying COLLATE pg_catalog."default",
    company_address_line_1 character varying COLLATE pg_catalog."default",
    company_city character varying COLLATE pg_catalog."default",
    company_state character varying COLLATE pg_catalog."default",
    company_country character varying COLLATE pg_catalog."default",
    company_postal_code character varying COLLATE pg_catalog."default",
    allotment_header_id character varying COLLATE pg_catalog."default",
    resv_name_id character varying COLLATE pg_catalog."default",
    etl_file_name character varying COLLATE pg_catalog."default"
)

TABLESPACE pg_default;
