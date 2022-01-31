DROP TABLE IF EXISTS t_row_count;
CREATE TEMPORARY TABLE t_row_count AS SELECT 'stage.stage_opera' table_name,COUNT(*) number_of_rows FROM stage.stage_opera;
INSERT INTO t_row_count SELECT 'stage_onq_crsstay',COUNT(*) FROM stage.stage_onq_crsstay;
INSERT INTO t_row_count SELECT 'stage_onq_pmsledger',COUNT(*) FROM stage.stage_onq_pmsledger;

INSERT INTO t_row_count SELECT 'lookup_property',COUNT(*) FROM lookup.lookup_property;
INSERT INTO t_row_count SELECT 'lookup_reservation',COUNT(*) FROM lookup.lookup_reservation;
INSERT INTO t_row_count SELECT 'reservation_stay_date_master_f',COUNT(*) FROM warehouse.reservation_stay_date_master_f;
INSERT INTO t_row_count SELECT 'reservation_stay_date_extension_f',COUNT(*) FROM warehouse.reservation_stay_date_extension_f;
INSERT INTO t_row_count SELECT 'reservation_stay_date_revenue_master_f',COUNT(*) FROM warehouse.reservation_stay_date_revenue_master_f;


select * from monitor.ingestion_tracker;
select * from stage.stage_batch;
SELECT * FROM t_row_count;
