DROP FUNCTION IF EXISTS lookup.f_process_stage_onq_pmsledger(bigint);
CREATE SCHEMA IF NOT EXISTS lookup;

CREATE OR REPLACE FUNCTION lookup.f_process_stage_onq_pmsledger(etlBatchId bigint)
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE sourceId integer;
DECLARE partitionRecord record;
DECLARE combinedSourceId integer;
BEGIN
    SELECT
        internal_source_id INTO sourceId
    FROM
        lookup.lookup_source
    WHERE
        source_key = 'ONQ_PMSLEDGER';
    SELECT
        internal_source_id INTO combinedSourceId
    FROM
        lookup.lookup_source
    WHERE
        source_key = 'ONQ_CRSSTAY';

    -- temp work table for this function. Gather all batches where source id matches onq pms ledger, stage is completed and warehouse is not completed and warehouse timestamp is null.
	DROP TABLE IF EXISTS t_stage_to_warehouse_onq_pmsledger;
	CREATE TEMPORARY TABLE t_stage_to_warehouse_onq_pmsledger AS
	SELECT
		o.*,
		combinedSourceId AS combined_source_id,
		CAST(0 AS bigint) master_id,
		CONCAT(confirmation_number,':',gnr,':',inncode) reservation_key,
		-1 internal_reservation_id,
		-1 internal_property_id
	FROM
		stage.stage_batch b
		JOIN stage.stage_onq_pmsledger o ON o.etl_batch_id = b.batch_id
	WHERE
		b.source_id = sourceId
		AND b.stage_completed_ind = true
		AND b.warehouse_completed_ind = false
		AND b.warehouse_transfer_start_timestamp IS NULL
		AND (b.batch_id = etlBatchId OR etlBatchId IS NULL);

    -- mark these records as picked up so that another run does not pick it.
	UPDATE
		stage.stage_batch b
	SET
		warehouse_transfer_start_timestamp = current_timestamp
	FROM
		t_stage_to_warehouse_onq_pmsledger o
	WHERE
		o.etl_batch_id = b.batch_id;
    -- START - GET internal reservation id and update in temp table.
	INSERT INTO lookup.lookup_reservation (lookup_reservation_key,source_id)
	SELECT DISTINCT
		s.reservation_key,
		s.combined_source_id
	FROM
		t_stage_to_warehouse_onq_pmsledger s
    WHERE
        confirmation_number IS NOT NULL
        AND gnr IS NOT NULL
        AND inncode IS NOT NULL
	ON CONFLICT (lookup_reservation_key,source_id)
	DO NOTHING;
	UPDATE
		t_stage_to_warehouse_onq_pmsledger t
	SET
		internal_reservation_id = l.internal_reservation_id
	FROM
		lookup.lookup_reservation l
	WHERE
		l.lookup_reservation_key = t.reservation_key
		AND l.source_id = t.combined_source_id;
    -- END - GET internal reservation id and update in temp table.

    -- START - GET internal property id and update in temp table.
	INSERT INTO lookup.lookup_property (property_code,source_id)
	SELECT DISTINCT
		UPPER(inncode),
		source_id
	FROM
		t_stage_to_warehouse_onq_pmsledger
    WHERE
        inncode IS NOT NULL
	ON CONFLICT (property_code,source_id)
	DO NOTHING;

	UPDATE
		t_stage_to_warehouse_onq_pmsledger t
	SET
		internal_property_id = l.internal_property_id
	FROM
		lookup.lookup_property l
	WHERE
		UPPER(l.property_code) = UPPER(t.inncode)
		AND l.source_id = t.source_id;
    -- START - Create partitions for all unique business date where business date is not null.
    FOR partitionRecord IN SELECT DISTINCT partition_date FROM t_stage_to_warehouse_onq_pmsledger WHERE business_date IS NOT NULL
        loop
            PERFORM warehouse.f_create_table_partition('warehouse.reservation_stay_date_revenue_master_f',CAST(partitionRecord.partition_date AS character varying));
        end loop;
    -- END - Create partitions for all unique business date where business date is not null.
    -- START - insert into warehouse tables.
    DROP TABLE IF EXISTS t_onq_ledger_summary;
    CREATE TEMPORARY TABLE t_onq_ledger_summary AS
    SELECT
        internal_reservation_id,
        partition_date,
        business_date,
        MIN(source_id) AS source_id,
        MIN(internal_property_id) as internal_property_id,
        CAST(0 AS decimal(38,10)) AS room_revenue,
        CAST(0 AS decimal(38,10)) AS food_revenue,
        CAST(0 AS decimal(38,10)) AS beverage_revenue,
        CAST(0 AS decimal(38,10)) AS phone_revenue,
        CAST(0 AS decimal(38,10)) AS shop_revenue,
        CAST(0 AS decimal(38,10)) AS other_revenue,
        CAST(0 AS decimal(38,10)) AS tax,
        MIN(etl_file_name) AS etl_file_name
    FROM
        t_stage_to_warehouse_onq_pmsledger
    WHERE
        internal_reservation_id > 0
        AND charge_category IS NOT NULL
    GROUP BY
        internal_reservation_id,
        partition_date,
        business_date;

    DROP TABLE IF EXISTS t_charge_category_total;
    CREATE TEMPORARY TABLE t_charge_category_total AS
    SELECT
        internal_reservation_id,
        partition_date,
        business_date,
        SUM(ledger_entry_amount) category_total
    FROM
        t_stage_to_warehouse_onq_pmsledger
    WHERE
        internal_reservation_id > 0
        AND charge_category = 'R'
    GROUP BY
        internal_reservation_id,
        partition_date,
        business_date;

    UPDATE
        t_onq_ledger_summary s
    SET
        room_revenue = category_total
    FROM
        t_charge_category_total c
    WHERE
        c.internal_reservation_id = s.internal_reservation_id
        AND c.business_date = s.business_date
        AND c.partition_date = s.partition_date;

    DROP TABLE IF EXISTS t_charge_category_total;
    CREATE TEMPORARY TABLE t_charge_category_total AS
    SELECT
        internal_reservation_id,
        partition_date,
        business_date,
        SUM(ledger_entry_amount) category_total
    FROM
        t_stage_to_warehouse_onq_pmsledger
    WHERE
        internal_reservation_id > 0
        AND charge_category = 'F'
    GROUP BY
        internal_reservation_id,
        partition_date,
        business_date;

    UPDATE
        t_onq_ledger_summary s
    SET
        food_revenue = category_total
    FROM
        t_charge_category_total c
    WHERE
        c.internal_reservation_id = s.internal_reservation_id
        AND c.business_date = s.business_date
        AND c.partition_date = s.partition_date;

    DROP TABLE IF EXISTS t_charge_category_total;
    CREATE TEMPORARY TABLE t_charge_category_total AS
    SELECT
        internal_reservation_id,
        partition_date,
        business_date,
        SUM(ledger_entry_amount) category_total
    FROM
        t_stage_to_warehouse_onq_pmsledger
    WHERE
        internal_reservation_id > 0
        AND charge_category = 'B'
    GROUP BY
        internal_reservation_id,
        partition_date,
        business_date;

    UPDATE
        t_onq_ledger_summary s
    SET
        beverage_revenue = category_total
    FROM
        t_charge_category_total c
    WHERE
        c.internal_reservation_id = s.internal_reservation_id
        AND c.business_date = s.business_date
        AND c.partition_date = s.partition_date;

    DROP TABLE IF EXISTS t_charge_category_total;
    CREATE TEMPORARY TABLE t_charge_category_total AS
    SELECT
        internal_reservation_id,
        partition_date,
        business_date,
        SUM(ledger_entry_amount) category_total
    FROM
        t_stage_to_warehouse_onq_pmsledger
    WHERE
        internal_reservation_id > 0
        AND charge_category = 'M'
    GROUP BY
        internal_reservation_id,
        partition_date,
        business_date;

    UPDATE
        t_onq_ledger_summary s
    SET
        other_revenue = category_total
    FROM
        t_charge_category_total c
    WHERE
        c.internal_reservation_id = s.internal_reservation_id
        AND c.business_date = s.business_date
        AND c.partition_date = s.partition_date;
    DROP TABLE IF EXISTS t_charge_category_total;
    CREATE TEMPORARY TABLE t_charge_category_total AS
    SELECT
        internal_reservation_id,
        partition_date,
        business_date,
        SUM(ledger_entry_amount) category_total
    FROM
        t_stage_to_warehouse_onq_pmsledger
    WHERE
        internal_reservation_id > 0
        AND charge_category = 'P'
    GROUP BY
        internal_reservation_id,
        partition_date,
        business_date;

    UPDATE
        t_onq_ledger_summary s
    SET
        phone_revenue = category_total
    FROM
        t_charge_category_total c
    WHERE
        c.internal_reservation_id = s.internal_reservation_id
        AND c.business_date = s.business_date
        AND c.partition_date = s.partition_date;

    DROP TABLE IF EXISTS t_charge_category_total;
    CREATE TEMPORARY TABLE t_charge_category_total AS
    SELECT
        internal_reservation_id,
        partition_date,
        business_date,
        SUM(ledger_entry_amount) category_total
    FROM
        t_stage_to_warehouse_onq_pmsledger
    WHERE
        internal_reservation_id > 0
        AND charge_category = 'S'
    GROUP BY
        internal_reservation_id,
        partition_date,
        business_date;

    UPDATE
        t_onq_ledger_summary s
    SET
        shop_revenue = category_total
    FROM
        t_charge_category_total c
    WHERE
        c.internal_reservation_id = s.internal_reservation_id
        AND c.business_date = s.business_date
        AND c.partition_date = s.partition_date;

    DROP TABLE IF EXISTS t_charge_category_total;
    CREATE TEMPORARY TABLE t_charge_category_total AS
    SELECT
        internal_reservation_id,
        partition_date,
        business_date,
        SUM(ledger_entry_amount) category_total
    FROM
        t_stage_to_warehouse_onq_pmsledger
    WHERE
        internal_reservation_id > 0
        AND charge_category = 'T'
    GROUP BY
        internal_reservation_id,
        partition_date,
        business_date;

    UPDATE
        t_onq_ledger_summary s
    SET
        tax = category_total
    FROM
        t_charge_category_total c
    WHERE
        c.internal_reservation_id = s.internal_reservation_id
        AND c.business_date = s.business_date
        AND c.partition_date = s.partition_date;
  INSERT INTO
        warehouse.reservation_stay_date_revenue_master_f
        (
            internal_stage_id,
            source_id,
            internal_reservation_id,
            internal_property_id,
            business_date,
            stay_date,
            room_revenue,
            food_revenue,
            beverage_revenue,
            phone_revenue,
            shop_revenue,
            other_revenue,
            tax,
            etl_file_name,
            etl_ingest_datetime
        )
    SELECT
        0,
        t.source_id,
        t.internal_reservation_id,
        t.internal_property_id,
        t.partition_date,
        t.business_date,
        t.room_revenue,
        t.food_revenue,
        t.beverage_revenue,
        t.phone_revenue,
        t.shop_revenue,
        t.other_revenue,
        t.tax,
        t.etl_file_name,
        current_timestamp
    FROM
        t_onq_ledger_summary t;


    return null;
END
$BODY$;
