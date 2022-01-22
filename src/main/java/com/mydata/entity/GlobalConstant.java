package com.mydata.entity;

public interface GlobalConstant {
    enum DB_CONNECTION_KEY {
        DB_UID,
        DB_PWD,
        DB_SECRETS_NAME,
        DB_CONNECTION_STRING
    }


    String ENV_LOG_TO_SYSTEM_OUT = "ENV_LOG_TO_SYSTEM_OUT";
    enum SOURCE_KEY {
        OPERA,
        ONQ_CRSSTAY,
        ONQ_PMSLEDGER
    }

    enum PSQL_PARAMETER_TYPE {
        CHARACTER_VARYING,
        INTEGER,
        DOUBLE,
        BIGINT,
        DATE,
        BOOLEAN,
        TIMESTAMP
    }

    enum ETL_COLUMN_NAME {
        etl_batch_id,
        etl_file_name,
        etl_ingest_datetime
    }

    enum SOURCE_TYPE {
        FILE
    }

    enum INGESTION_STATUS {
        INGESTION_FAILED(-6),
        RDZ_WRITE_FAILED(-5),
        WAREHOUSE_WRITE_FAILED(-4),
        STAGE_WRITE_FAILED(-3),
        S3_STREAM_READ_FAILED(-2),
        SOURCE_RECEIVED(1),
        S3_STREAM_READ_COMPLETE(2),
        STAGE_WRITE_COMPLETE(3),
        WAREHOUSE_WRITE_COMPLETE(4),
        RDZ_WRITE_COMPLETE(5),
        INGESTION_COMPLETE(6);
        private final int value;
        INGESTION_STATUS(int value) {
            this.value = value;
        }
        public int getValue() {
            return value;
        }
    }

    /*
1	"SOURCE_RECEIVED"
2	"STAGE_WRITE_COMPLETE"
3	"WAREHOUSE_WRITE_COMPLETE"
4	"RDZ_WRITE_COMPLETE"
5	"INGESTION_COMPLETE"
     */
}
