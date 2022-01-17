-- FUNCTION: monitor.f_check_row_count(character varying)
CREATE SCHEMA IF NOT EXISTS monitor;
DROP FUNCTION IF EXISTS monitor.f_check_row_count();
CREATE OR REPLACE FUNCTION monitor.f_check_row_count()
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
BEGIN
    PERFORM SELECT COUNT(*) FROM warehouse.reservation_stay_day_f;
END;
$BODY$;
