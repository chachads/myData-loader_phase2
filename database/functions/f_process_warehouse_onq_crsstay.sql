-- FUNCTION: lookup.f_cron_process_warehouse_onq_crsstay(character varying)

DROP FUNCTION IF EXISTS lookup.f_cron_process_warehouse_onq_crsstay();
CREATE SCHEMA IF NOT EXISTS lookup;

CREATE OR REPLACE FUNCTION lookup.f_cron_process_warehouse_onq_crsstay()
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE sourceId integer;
DECLARE partitionRecord record;

BEGIN
    SELECT
        internal_source_id INTO sourceId
    FROM
        lookup.lookup_source
    WHERE
        source_key = 'ONQ_CRSSTAY';
    -- temp work table for this function. Gather all batches where source id matches onq crs stay, stage is completed and warehouse is not completed and warehouse timestamp is null.
	DROP TABLE IF EXISTS t_stage_to_warehouse_onq_crsstay;
	CREATE TEMPORARY TABLE t_stage_to_warehouse_onq_crsstay AS
	SELECT
		o.*,
		CONCAT(confirmation_number,':',gnr,':',partition_date) reservation_key,
		-1 internal_reservation_id,
		-1 internal_property_id
	FROM
		stage.stage_batch b
		JOIN stage.stage_onq_crsstay o ON o.etl_batch_id = b.batch_id
	WHERE
		b.source_id = sourceId
		AND b.stage_completed_ind = true
		AND b.warehouse_completed_ind = false
		AND b.warehouse_transfer_start_timestamp IS NULL;
    -- mark these records as picked up so that another run does not pick it.
	UPDATE
		stage.stage_batch b
	SET
		warehouse_transfer_start_timestamp = current_timestamp
	FROM
		t_stage_to_warehouse_onq_crsstay o
	WHERE
		o.etl_batch_id = b.batch_id;
    -- START - GET internal reservation id and update in temp table.
	INSERT INTO lookup.lookup_reservation (lookup_reservation_key,source_id)
	SELECT DISTINCT
		s.reservation_key,
		s.source_id
	FROM
		t_stage_to_warehouse_onq_crsstay s
	ON CONFLICT (lookup_reservation_key,source_id)
	DO NOTHING;
	UPDATE
		t_stage_to_warehouse_onq_crsstay t
	SET
		internal_reservation_id = l.internal_reservation_id
	FROM
		lookup.lookup_reservation l
	WHERE
		l.lookup_reservation_key = t.reservation_key
		AND l.source_id = t.source_id;
    -- END - GET internal reservation id and update in temp table.

    -- START - GET internal property id and update in temp table.
	INSERT INTO lookup.lookup_property (property_code,source_id)
	SELECT DISTINCT
		UPPER(crs_inn_code),
		source_id
	FROM
		t_stage_to_warehouse_onq_crsstay
	ON CONFLICT (property_code,source_id)
	DO NOTHING;

	UPDATE
		t_stage_to_warehouse_onq_crsstay t
	SET
		internal_property_id = l.internal_property_id
	FROM
		lookup.lookup_property l
	WHERE
		UPPER(l.property_code) = UPPER(t.crs_inn_code)
		AND l.source_id = t.source_id;
    -- START - GET internal property id and update in temp table.
    -- START - Create partitions for all unique business date where business date is not null.
    FOR partitionRecord IN SELECT DISTINCT partition_date FROM t_stage_to_warehouse_onq_crsstay WHERE partition_date IS NOT NULL
        loop
            PERFORM warehouse.f_create_table_partition('warehouse.reservation_stay_date_f',CAST(partitionRecord.partition_date AS character varying));
            PERFORM warehouse.f_create_table_partition('warehouse.reservation_business_date_f',CAST(partitionRecord.partition_date AS character varying));
            PERFORM warehouse.f_create_table_partition('warehouse.reservation_business_date_extension_f',CAST(partitionRecord.partition_date AS character varying));
        end loop;
    -- END - Create partitions for all unique business date where business date is not null.

		INSERT INTO warehouse.reservation_stay_date_f
    	(
            source_id,
            internal_reservation_id,
            internal_property_id,
            business_date,
            stay_date,
            arrival_date,
            departure_date,
            rate_code,
            rate_code_description,
            room_rate,
            currency_code,
            room_type,
            etl_file_name,
            etl_ingest_datetime
        )
    	SELECT
            sourceId,
            internal_reservation_id,
            internal_property_id,
            s.partition_date,
            s.stay_date,
            s.arrival_date,
            s.departure_date,
            s.srp_code,
            s.srp_name,
            s.prop_crs_room_rate,
            s.prop_currency_code,
            s.room_type_code,
            s.etl_file_name,
            current_timestamp
    	FROM
    		t_stage_to_warehouse_onq_crsstay s;

    UPDATE
        stage.stage_batch b
    SET
        warehouse_completed_ind = true,
        warehouse_transfer_completed_timestamp = current_timestamp
    FROM
            t_stage_to_warehouse_onq_crsstay o
        WHERE
            o.etl_batch_id = b.batch_id;
    -- END - insert into warehouse tables.
    -- START CLEAN UP
    DROP TABLE IF EXISTS t_distinct_etl_batch_ids;
    CREATE TEMPORARY TABLE
        t_distinct_etl_batch_ids AS
    SELECT
        DISTINCT etl_batch_id
    FROM
        t_stage_to_warehouse_onq_crsstay;
                     
    DELETE FROM
        stage.stage_onq_crsstay s
    USING
       t_distinct_etl_batch_ids t
    WHERE
        s.etl_batch_id = t.etl_batch_id;

    return null;
END
$BODY$;
