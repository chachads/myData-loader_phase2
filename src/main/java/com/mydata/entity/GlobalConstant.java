package com.mydata.entity;

public interface GlobalConstant {
    enum DB_CONNECTION_KEY {
        DB_UID,
        DB_PWD,
        DB_SECRETS_NAME,
        DB_CONNECTION_STRING
    }

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
}
