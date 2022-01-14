package com.mydata.entity;

import com.mydata.helper.DBHelper;

/**
 * Handles all DB request. This is required in constructor of {@link DBHelper}
 */
public class DBHelperRequest {
    private String dbURL;
    private String dbUID;
    private String dbPWD;


    public DBHelperRequest() {
        this.dbURL = System.getenv(GlobalConstant.DB_CONNECTION_KEY.DB_URL.toString());
        this.dbPWD = System.getenv(GlobalConstant.DB_CONNECTION_KEY.DB_PWD.toString());
        this.dbUID = System.getenv(GlobalConstant.DB_CONNECTION_KEY.DB_UID.toString());
    }

    public DBHelperRequest(String dbURL, String dbUID, String dbPWD) {
        this.dbURL = dbURL;
        this.dbUID = dbUID;
        this.dbPWD = dbPWD;
    }

    public String getDbURL() {
        return dbURL;
    }

    public String getDbUID() {
        return dbUID;
    }

    public String getDbPWD() {
        return dbPWD;
    }

}
