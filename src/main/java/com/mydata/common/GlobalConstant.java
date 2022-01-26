package com.mydata.common;

import java.util.HashMap;
import java.util.Map;

public interface GlobalConstant {
    enum DB_CONNECTION_KEY {
        DB_UID,
        DB_PWD,
        DB_SECRETS_NAME,
        DB_CONNECTION_STRING
    }
    //String ENV_LOG_TO_SYSTEM_OUT = "ENV_LOG_TO_SYSTEM_OUT";
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
        INGESTION_FAILED(-6,"Ingestion Failed."),
        RDZ_WRITE_FAILED(-5,"Write to RDZ failed."),
        WAREHOUSE_WRITE_FAILED(-4,"Write to warehouse tables failed."),
        STAGE_WRITE_FAILED(-3,"Write to stage tables failed."),
        S3_STREAM_READ_FAILED(-2,"Stream read from S3 failed."),
        SOURCE_RECEIVED(1,"S3 Trigger received."),
        S3_STREAM_READ_COMPLETE(2,"Stream read from S3 completed."),
        STAGE_WRITE_COMPLETE(3,"Write to stage tables completed."),
        WAREHOUSE_WRITE_COMPLETE(4,"Write to warehouse tables completed."),
        RDZ_WRITE_COMPLETE(5,"Write to RDZ completed."),
        INGESTION_COMPLETE(6,"Ingestion Completed.");

        private static final Map<Integer, INGESTION_STATUS> BY_INGESTION_ID = new HashMap<>();
        private static final Map<String, INGESTION_STATUS> BY_STATUS_MESSAGE = new HashMap<>();

        static {
            for (INGESTION_STATUS e : values()) {
                BY_INGESTION_ID.put(e.ingestionId, e);
                BY_STATUS_MESSAGE.put(e.statusMessage, e);
            }
        }

        public final int ingestionId;
        public final String statusMessage;

        INGESTION_STATUS(int ingestionId, String statusMessage) {
            this.ingestionId = ingestionId;
            this.statusMessage = statusMessage;
        }


        public static INGESTION_STATUS valueOfIngestionId(int number) {
            return BY_INGESTION_ID.get(number);
        }

        public static INGESTION_STATUS valueOfStatusMessage(String statusMessage) {
            return BY_STATUS_MESSAGE.get(statusMessage);
        }
    }
    Boolean ENV_LOG_TO_SYSTEM_OUT = Boolean.parseBoolean(System.getenv("ENV_LOG_TO_SYSTEM_OUT"));

    Boolean ENV_USE_CRON_TO_LOAD_WAREHOUSE = Boolean.parseBoolean(System.getenv("ENV_USE_CRON_TO_LOAD_WAREHOUSE"));
}
