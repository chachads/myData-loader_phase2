package com.mydata.entity;

import com.mydata.entity.domain.IngestSourceDetail;

import java.util.List;

/**
 * Global config object. Holds various attributes like DBUrls etc.
 */
public class GlobalConfig {
    private String dbURL;
    private String dbUID;
    private String dbPWD;
    private List<IngestSourceDetail> ingestSourceDetailList;

    /**
     * Postgres DB URL. Read from s3://{bucket-name}/config/mydata.properties
     *
     * @return - returns the database URL
     */
    public String getDbURL() {
        return dbURL;
    }

    /**
     * Postgres DB URL. Read from s3://{bucket-name}/config/mydata.properties
     *
     * @param dbURL - database URL
     */
    public void setDbURL(String dbURL) {
        this.dbURL = dbURL;
    }

    /**
     * Postgres DB UID. Read from s3://{bucket-name}/config/mydata.properties
     *
     * @return - returns the database user id
     */
    public String getDbUID() {
        return dbUID;
    }

    /**
     * Postgres DB UID. Read from s3://{bucket-name}/config/mydata.properties
     *
     * @param dbUID - database user id
     */
    public void setDbUID(String dbUID) {
        this.dbUID = dbUID;
    }

    /**
     * Postgres DB PWD. Read from s3://{bucket-name}/config/mydata.properties
     *
     * @return - returns the database password
     */
    public String getDbPWD() {
        return dbPWD;
    }

    /**
     * Postgres DB PWD. Read from s3://{bucket-name}/config/mydata.properties
     *
     * @param dbPWD - database password
     */
    public void setDbPWD(String dbPWD) {
        this.dbPWD = dbPWD;
    }

    public List<IngestSourceDetail> getIngestSourceDetailList() {
        return ingestSourceDetailList;
    }

    public void setIngestSourceDetailList(List<IngestSourceDetail> ingestSourceDetailList) {
        this.ingestSourceDetailList = ingestSourceDetailList;
    }
}
