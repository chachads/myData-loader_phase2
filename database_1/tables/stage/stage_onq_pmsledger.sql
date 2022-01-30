-- Table: stage.stage_onq_pmsledger
CREATE SCHEMA IF NOT EXISTS stage;

DROP TABLE IF EXISTS stage.stage_onq_pmsledger CASCADE;

CREATE TABLE IF NOT EXISTS stage.stage_onq_pmsledger
(
    etl_batch_id bigint,
    accounting_category character varying COLLATE pg_catalog."default" null,
    accounting_id character varying COLLATE pg_catalog."default" null,
    accounting_id_desc character varying COLLATE pg_catalog."default" null,
    accounting_type character varying COLLATE pg_catalog."default" null,
    ar_account_id character varying COLLATE pg_catalog."default" null,
    ar_account_key character varying COLLATE pg_catalog."default" null,
    ar_code character varying COLLATE pg_catalog."default" null,
    ar_description character varying COLLATE pg_catalog."default" null,
    ar_type_code character varying COLLATE pg_catalog."default" null,
    ar_type_sub_code character varying COLLATE pg_catalog."default" null,
    business_date date null,
    charge_category character varying COLLATE pg_catalog."default" null,
    charge_routed character varying COLLATE pg_catalog."default" null,
    common_account_identifier character varying COLLATE pg_catalog."default" null,
    confirmation_number character varying COLLATE pg_catalog."default" null,
    crs_inn_code character varying COLLATE pg_catalog."default" null,
    employee_id character varying COLLATE pg_catalog."default" null,
    entry_currency_code character varying COLLATE pg_catalog."default" null,
    entry_datetime timestamp without time zone null,
    entry_id bigint null,
    entry_type character varying COLLATE pg_catalog."default" null,
    exchange_rate decimal(38,10) null,
    facility_id character varying COLLATE pg_catalog."default" null,
    foreign_amount decimal(38,10) null,
    gl_account_id character varying COLLATE pg_catalog."default" null,
    gnr character varying COLLATE pg_catalog."default" null,
    group_key character varying COLLATE pg_catalog."default" null,
    group_name character varying COLLATE pg_catalog."default" null,
    hhonors_receipt_ind boolean null,
    house_key character varying COLLATE pg_catalog."default" null,
    include_in_net_use character varying COLLATE pg_catalog."default" null,
    inncode character varying COLLATE pg_catalog."default" null,
    ledger_entry_amount decimal(38,10) null,
    original_folio_id bigint null,
    original_receipt_id bigint null,
    original_stay_id bigint null,
    owner_account_id character varying COLLATE pg_catalog."default" null,
    owner_account_name character varying COLLATE pg_catalog."default" null,
    owner_extract_type character varying COLLATE pg_catalog."default" null,
    partition_date date null,
    pms_inn_code character varying COLLATE pg_catalog."default" null,
    posting_type_code character varying COLLATE pg_catalog."default" null,
    receipt_id bigint null,
    routed_to_folio bigint null,
    stay_id bigint null,
    trans_desc character varying COLLATE pg_catalog."default" null,
    trans_id bigint null,
    trans_travel_reason_code character varying COLLATE pg_catalog."default" null,
    version character varying COLLATE pg_catalog."default" null,
    source_id int,
    etl_file_name character varying COLLATE pg_catalog."default" null,
    etl_ingest_datetime timestamp without time zone
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS stage.stage_onq_pmsledger
    OWNER to postgres;