package com.mydata.entity.domain;

import com.mydata.entity.DBHelperRequest;
import com.mydata.entity.GlobalConstant;
import com.mydata.entity.S3HelperRequest;
import com.mydata.entity.tracker.SourceTrackerDetail;
import com.mydata.helper.DBHelper;
import com.mydata.helper.IDBHelper;

import java.sql.Connection;
import java.sql.SQLException;

public class IngestSourceDetail {
    private final String fromBucket;
    private final String s3EventPrefix;
    private String tempTableDefinition;
    private String stageTableName;
    private String rawFileName;
    private S3HelperRequest s3HelperRequest;
    private String sourceFormat;
    private String toBucket;
    private String sourceKey;
    private IDBHelper dbHelper;
    private String localFilePath;
    private SourceTrackerDetail sourceTrackerDetail;
    private Integer dbSourceId;
    private Connection lambdaDBConnection;
    private String sourceDateFormat;
    private final Boolean setupDB;

    public IngestSourceDetail(String s3EventPrefix, String fromBucket) {
        this.s3EventPrefix = s3EventPrefix;
        this.fromBucket = fromBucket;
        setupDB = false;
        setupIngestionDetail();
    }

    public IngestSourceDetail(String s3EventPrefix, String fromBucket, Connection lambdaDBConnection) {
        this.s3EventPrefix = s3EventPrefix;
        this.fromBucket = fromBucket;
        this.lambdaDBConnection = lambdaDBConnection;
        setupDB = true;
        setupIngestionDetail();
    }

    public Connection getLambdaDBConnection() {
        return lambdaDBConnection;
    }

    public String getSourceDateFormat() {
        return sourceDateFormat;
    }

    public void setSourceDateFormat(String sourceDateFormat) {
        this.sourceDateFormat = sourceDateFormat;
    }

    public String getTempTableDefinition() {
        return tempTableDefinition;
    }

    public void setTempTableDefinition(String tempTableDefinition) {
        this.tempTableDefinition = tempTableDefinition;
    }

    public String getStageTableName() {
        return stageTableName;
    }

    public void setStageTableName(String stageTableName) {
        this.stageTableName = stageTableName;
    }


    public String getRawFileName() {
        return rawFileName;
    }


    public String getSourceFormat() {
        return sourceFormat;
    }

    public void setSourceFormat(String sourceFormat) {
        this.sourceFormat = sourceFormat;
    }

    public String getSourceKey() {
        return sourceKey;
    }

    public S3HelperRequest getS3HelperRequest() {
        return s3HelperRequest;
    }

    public IDBHelper getDbHelper() {
        if (dbHelper == null) {
            DBHelperRequest dbHelperRequest = System.getenv().containsKey(GlobalConstant.DB_CONNECTION_KEY.DB_SECRETS_NAME.toString()) ? new DBHelperRequest(System.getenv(GlobalConstant.DB_CONNECTION_KEY.DB_SECRETS_NAME.toString())) : new DBHelperRequest();
            dbHelper = new DBHelper(dbHelperRequest, lambdaDBConnection);
        }
        return dbHelper;
    }

    public String getLocalFilePath() {
        return localFilePath;
    }

    public void setLocalFilePath(String localFilePath) {
        this.localFilePath = localFilePath;
    }

    public SourceTrackerDetail getFileTrackerDetail() {
        return sourceTrackerDetail;
    }

    public void setFileTrackerDetail(SourceTrackerDetail sourceTrackerDetail) {
        this.sourceTrackerDetail = sourceTrackerDetail;
    }

    protected void setupIngestionDetail() {
        System.out.println("Started capture s3 details");
        String[] fileNameArray = s3EventPrefix.split("/");

        rawFileName = fileNameArray[fileNameArray.length - 1];
        String toKey = String.format("rdz/%s", rawFileName);
        s3HelperRequest = new S3HelperRequest();
        s3HelperRequest.setFromBucket(fromBucket);
        s3HelperRequest.setFromKey(s3EventPrefix);
        if (toBucket == null || toBucket.isEmpty() || toBucket.trim().isEmpty())
            toBucket = fromBucket;
        s3HelperRequest.setToBucket(toBucket);
        s3HelperRequest.setToKey(toKey);
        s3HelperRequest.setFileName(rawFileName);
        sourceKey = s3EventPrefix.replace("ldz", "").replace(rawFileName, "").replace("/", "");
        s3HelperRequest.setSourceTypeKey(GlobalConstant.SOURCE_KEY.valueOf(sourceKey));
        if (setupDB) {
            // Set up the database fields based on the source type key.
            try {
                getDbHelper().refreshSourceDefinition(this);
            } catch (SQLException ex) {

            }
        }
        sourceTrackerDetail = new SourceTrackerDetail(GlobalConstant.SOURCE_TYPE.FILE, sourceKey, rawFileName, fromBucket, s3EventPrefix, toBucket, toKey, stageTableName);
    }

    public GlobalConstant.SOURCE_KEY getESourceKey() {
        return GlobalConstant.SOURCE_KEY.valueOf(sourceKey);
    }

    public Integer getDbSourceId() {
        return dbSourceId;
    }

    public void setDbSourceId(Integer dbSourceId) {
        this.dbSourceId = dbSourceId;
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
        return
                String.format("tempTableDefinition: %s. stageTableName: %s. rawFileName: %s. sourceKey:%s. sourceFormat: %s", tempTableDefinition, stageTableName, rawFileName, sourceKey, sourceFormat);
    }
}
