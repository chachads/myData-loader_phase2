DROP VIEW IF EXISTS warehouse.vw_reservation_stay_date_master;
CREATE VIEW warehouse.vw_reservation_stay_date_master AS
SELECT
    *
FROM
    warehouse.reservation_stay_date_master_f;