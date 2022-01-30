DROP VIEW IF EXISTS warehouse.vw_reservation_stay_day_header_f CASCADE;

CREATE VIEW warehouse.vw_reservation_stay_day_header_f AS

SELECT
	min(record_id) header_record_id,
	business_date,
	internal_reservation_id
FROM
	warehouse.reservation_stay_date_f
GROUP BY
	internal_reservation_id
	,business_date;