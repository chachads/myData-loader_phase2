package com.mydata.entity;

import com.mydata.helper.DBHelper;

/**
 * Handles all DB request. This is required in constructor of {@link DBHelper}
 */
public class DBHelperRequest {
    private String dbHost;
    private String dbProxyHost;
    private String dbURL;
    private String dbUID;
    private String dbPWD;
    private String dbName;
    private Integer dbPort;
    private String secretsName;
    private Boolean dbSecretsRefreshed;
    private Boolean useSecretsManager;
//jdbc:postgresql://mydata-poc-defvpc.cgzrqpakxtee.us-east-1.rds.amazonaws.com:5432/mydata

    public DBHelperRequest(String secretsName) {
        this.secretsName = secretsName;
        this.dbSecretsRefreshed = false;
        this.useSecretsManager = true;
    }

    public DBHelperRequest() {
        this.dbURL = System.getenv(GlobalConstant.DB_CONNECTION_KEY.DB_URL.toString());
        this.dbPWD = System.getenv(GlobalConstant.DB_CONNECTION_KEY.DB_PWD.toString());
        this.dbUID = System.getenv(GlobalConstant.DB_CONNECTION_KEY.DB_UID.toString());
        this.dbSecretsRefreshed = true;
    }

    public DBHelperRequest(String dbURL, String dbUID, String dbPWD) {
        this.dbURL = dbURL;
        this.dbUID = dbUID;
        this.dbPWD = dbPWD;
        this.dbSecretsRefreshed = true;
    }

    public String getDbHost() {
        return dbHost;
    }

    public void setDbHost(String dbHost) {
        this.dbHost = dbHost;
    }

    public String getDbProxyHost() {
        return dbProxyHost;
    }

    public void setDbProxyHost(String dbProxyHost) {
        this.dbProxyHost = dbProxyHost;
    }

    public String getDbURL() {
        String secretURL = String.format("jdbc:postgresql://%s:%d/%s", dbProxyHost != null ? dbProxyHost : dbHost, dbPort, dbName);
        return useSecretsManager ? secretURL : dbURL;
    }

    public String getDbUID() {
        return dbUID;
    }

    public String getDbPWD() {
        return dbPWD;
    }


    public void setDbUID(String dbUID) {
        this.dbUID = dbUID;
    }

    public void setDbPWD(String dbPWD) {
        this.dbPWD = dbPWD;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public Integer getDbPort() {
        return dbPort;
    }

    public void setDbPort(Integer dbPort) {
        this.dbPort = dbPort;
    }

    public String getSecretsName() {
        return secretsName;
    }

    public Boolean getDbSecretsRefreshed() {
        return dbSecretsRefreshed;
    }

    public void setDbSecretsRefreshed(Boolean dbSecretsRefreshed) {
        this.dbSecretsRefreshed = dbSecretsRefreshed;
    }

    /**
     * Returns a string representation of the object. In general, the
     * {@code toString} method returns a string that
     * "textually represents" this object. The result should
     * be a concise but informative representation that is easy for a
     * person to read.
     * It is recommended that all subclasses override this method.
     * <p>
     * The {@code toString} method for class {@code Object}
     * returns a string consisting of the name of the class of which the
     * object is an instance, the at-sign character `{@code @}', and
     * the unsigned hexadecimal representation of the hash code of the
     * object. In other words, this method returns a string equal to the
     * value of:
     * <blockquote>
     * <pre>
     * getClass().getName() + '@' + Integer.toHexString(hashCode())
     * </pre></blockquote>
     *
     * @return a string representation of the object.
     */
    @Override
    public String toString() {
        return String.format("dbURL: %s UID: %s PWD: %s DBNAME: %s DBPORT: %s SECRETSNAME: %s DBSECECTREFRESH:%b useSecretManager: %b", getDbURL(), getDbUID(), getDbPWD(), getDbName(), getDbPort(), getSecretsName(), getDbSecretsRefreshed(), useSecretsManager);
    }
}
