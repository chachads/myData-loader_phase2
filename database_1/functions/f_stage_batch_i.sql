-- FUNCTION: stage.f_stage_batch_i(character varying)

DROP FUNCTION IF EXISTS stage.f_stage_batch_i(integer,character varying);
CREATE SCHEMA IF NOT EXISTS stage;

CREATE OR REPLACE FUNCTION stage.f_stage_batch_i(
	sourceId int,
	etlFileName character varying)
    RETURNS bigint
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE batchId integer;
BEGIN
    INSERT INTO
        stage.stage_batch
        (
            source_id,
            etl_file_name
        )
    VALUES
        (
            sourceId,
            UPPER(etlFileName)
        );
    SELECT lastval() INTO batchId;
    return batchId;
END;
$BODY$;
