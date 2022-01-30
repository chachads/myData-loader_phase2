-- FUNCTION: lookup.f_ingestion_tracker_iu(character varying)
DROP FUNCTION IF EXISTS monitor.f_ingestion_tracker_iu(bigint,character varying,character varying,character varying,character varying,character varying,character varying,character varying,character varying,integer,timestamp,timestamp,timestamp,timestamp,integer,character varying);
CREATE SCHEMA IF NOT EXISTS monitor;
CREATE OR REPLACE FUNCTION monitor.f_ingestion_tracker_iu(
    sourceTrackerId bigint,
    sourceKey character varying,
    sourceType character varying,
    rawFileName  character varying,
    sourceBucket character varying,
    sourcePrefix character varying,
    targetBucket  character varying,
    targetPrefix character varying,
    stageDBTable character varying,
    insertRowCount integer,
    lambdaEventTriggerTime timestamp,
    stageWriteTime timestamp,
    warehouseWriteTime timestamp,
    rdzWriteTime timestamp,
    ingestionStatus integer,
    statusMessage character varying)
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
BEGIN
	if (sourceTrackerId IS NULL) THEN
            INSERT INTO
                monitor.ingestion_tracker
                (
	                source_key,
	                source_type,
	                raw_file_name,
	                source_bucket,
	                source_prefix,
	                target_bucket,
	                target_prefix,
	                stage_db_table,
	                insert_row_count,
                    lambda_event_trigger_time,
                    stage_write_time,
                    warehouse_write_time,
                    rdz_write_time,
                    ingestion_status_id,
                    status_message
	           )
	        VALUES (
	            sourceKey,
	            sourceType,
	            rawFileName,
	            sourceBucket,
	            sourcePrefix,
	            targetBucket,
	            targetPrefix,
	            stageDBTable,
	            insertRowCount,
	            lambdaEventTriggerTime,
	            stageWriteTime,
	            warehouseWriteTime,
	            rdzWriteTime,
	            ingestionStatus,
	            statusMessage
	        );
	        SELECT currval(pg_get_serial_sequence('monitor.ingestion_tracker', 'ingestion_tracker_id')) INTO sourceTrackerId;
	    END IF;

	    SELECT
            COALESCE(sourceKey,source_key),
            COALESCE(sourceType,source_type),
            COALESCE(rawFileName,raw_file_name),
            COALESCE(sourceBucket,source_bucket),
            COALESCE(sourcePrefix,source_prefix),
            COALESCE(targetBucket,target_bucket),
            COALESCE(targetPrefix,target_prefix),
            COALESCE(stageDBTable,stage_db_table),
            COALESCE(insertRowCount,insert_row_count),
            COALESCE(lambdaEventTriggerTime,lambda_event_trigger_time),
            COALESCE(stageWriteTime,stage_write_time),
            COALESCE(warehouseWriteTime,warehouse_write_time),
            COALESCE(rdzWriteTime,rdz_write_time),
            COALESCE(ingestionStatus,ingestion_status_id),
            COALESCE(statusMessage,status_message)
        INTO
            sourceKey
            sourceType,
            rawFileName,
            sourceBucket,
            sourcePrefix,
            targetBucket,
            targetPrefix,
            stageDBTable,
            insertRowCount,
            lambdaEventTriggerTime,
            stageWriteTime,
            warehouseWriteTime,
            rdzWriteTime,
            ingestionStatus,
            statusMessage
        FROM
            monitor.ingestion_tracker
        WHERE
            ingestion_tracker_id = sourceTrackerId;

        UPDATE
            monitor.ingestion_tracker
        SET
            source_key=sourceKey,
            source_type=sourceType,
            raw_file_name=rawFileName,
            source_bucket=sourceBucket,
            source_prefix=sourcePrefix,
            target_bucket=targetBucket,
            target_prefix=targetPrefix,
            stage_db_table=stageDBTable,
            insert_row_count=insertRowCount,
            lambda_event_trigger_time=lambdaEventTriggerTime,
            stage_write_time=stageWriteTime,
            warehouse_write_time = warehouseWriteTime,
            rdz_write_time=rdzWriteTime,
            ingestion_status_id=ingestionStatus,
            status_message = statusMessage
        WHERE
            ingestion_tracker_id = sourceTrackerId;

    return sourceTrackerId;
END;
$BODY$;


