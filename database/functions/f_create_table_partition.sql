-- FUNCTION: warehouse.f_create_table_partition(character varying)
CREATE SCHEMA IF NOT EXISTS warehouse;

DROP FUNCTION IF EXISTS warehouse.f_create_table_partition(character varying);

CREATE OR REPLACE FUNCTION warehouse.f_create_table_partition(
	tableName character varying,
	partitionValue character varying)
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE partitionTableName character varying;
DECLARE partitionSQL character varying;
BEGIN
    partitionTableName = tableName || '_' || regexp_replace(partitionValue, '[^\w]+','','g');
    partitionSQL = 'CREATE TABLE IF NOT EXISTS ' || partitionTableName || ' PARTITION OF ' || tableName || ' FOR VALUES IN (''' || partitionValue || '''' || ')';
    EXECUTE (partitionSQL);
    return 0;
END;
$BODY$;