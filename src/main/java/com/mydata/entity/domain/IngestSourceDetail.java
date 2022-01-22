package com.mydata.entity.domain;

import com.mydata.entity.GlobalConstant;
import com.mydata.entity.S3HelperRequest;
import com.mydata.entity.tracker.IngestionTrackerDetail;
import com.mydata.helper.CommonUtils;

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
    private String localFilePath;
    private IngestionTrackerDetail ingestionTrackerDetail;
    private Integer dbSourceId;
    private String sourceDateFormat;
    private String statusMessage;

    public IngestSourceDetail(String s3EventPrefix, String fromBucket) {
        this.s3EventPrefix = s3EventPrefix;
        this.fromBucket = fromBucket;
        setupIngestionDetail();
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


    public String getLocalFilePath() {
        return localFilePath;
    }

    public void setLocalFilePath(String localFilePath) {
        this.localFilePath = localFilePath;
    }

    public IngestionTrackerDetail getIngestionTrackerDetail() {
        return ingestionTrackerDetail;
    }

    public void setIngestionTrackerDetail(IngestionTrackerDetail ingestionTrackerDetail) {
        this.ingestionTrackerDetail = ingestionTrackerDetail;
    }

    protected void setupIngestionDetail() {
        CommonUtils.LogToSystemOut("Started capture s3 details");
        String[] fileNameArray = s3EventPrefix.split("/");

        rawFileName = fileNameArray[fileNameArray.length - 1];
        sourceKey = s3EventPrefix.replace("ldz", "").replace(rawFileName, "").replace("/", "");
        // RDZ format = rdz/currentdate/source/rawfile
        String rdzFormat = "rdz/%s/%s/%s";
        String toKey = String.format(rdzFormat, CommonUtils.getDateStr("yyyyMMdd",null), sourceKey,rawFileName);
        s3HelperRequest = new S3HelperRequest();
        s3HelperRequest.setFromBucket(fromBucket);
        s3HelperRequest.setFromKey(s3EventPrefix);
        if (toBucket == null || toBucket.isEmpty() || toBucket.trim().isEmpty())
            toBucket = fromBucket;
        s3HelperRequest.setToBucket(toBucket);
        s3HelperRequest.setToKey(toKey);
        s3HelperRequest.setFileName(rawFileName);
        s3HelperRequest.setSourceTypeKey(GlobalConstant.SOURCE_KEY.valueOf(sourceKey));
        ingestionTrackerDetail = new IngestionTrackerDetail(GlobalConstant.SOURCE_TYPE.FILE, sourceKey, rawFileName, fromBucket, s3EventPrefix, toBucket, toKey, stageTableName, GlobalConstant.INGESTION_STATUS.SOURCE_RECEIVED);
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


    public String getStatusMessage() {
        return statusMessage;
    }

    public void setStatusMessage(String statusMessage) {
        this.statusMessage = statusMessage;
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
