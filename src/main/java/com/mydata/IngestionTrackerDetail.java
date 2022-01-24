package com.mydata;

import com.mydata.common.GlobalConstant;

import java.sql.Timestamp;

public class IngestionTrackerDetail {
    private Long sourceTrackerId;
    private final String sourceKey;
    private final String rawFileName;
    private final String sourceBucket;
    private final String sourcePrefix;
    private final String targetBucket;
    private final String targetPrefix;
    private String stageDBTable;
    private Integer insertRowCount;
    private final Timestamp lambdaEventTriggerTime;
    private Timestamp stageWriteTime;
    private Timestamp warehouseWriteTime;
    private Timestamp rdzWriteTime;
    private final GlobalConstant.SOURCE_TYPE sourceType;
    private GlobalConstant.INGESTION_STATUS eIngestionStatus;
    private String statusMessage;

    public IngestionTrackerDetail(GlobalConstant.SOURCE_TYPE sourceType, String sourceKey, String rawFileName, String sourceBucket, String sourcePrefix, String targetBucket, String targetPrefix, String stageDBTable, GlobalConstant.INGESTION_STATUS eIngestionStatus) {
        this.sourceType = sourceType;
        this.sourceKey = sourceKey;
        this.rawFileName = rawFileName;
        this.sourceBucket = sourceBucket;
        this.sourcePrefix = sourcePrefix;
        this.targetBucket = targetBucket;
        this.targetPrefix = targetPrefix;
        this.stageDBTable = stageDBTable;
        this.eIngestionStatus = eIngestionStatus;
        this.lambdaEventTriggerTime = new Timestamp(System.currentTimeMillis());
        this.statusMessage = eIngestionStatus.statusMessage;
    }

    public String getSourceKey() {
        return sourceKey;
    }

    public Long getSourceTrackerId() {
        return sourceTrackerId;
    }

    public void setSourceTrackerId(Long sourceTrackerId) {
        this.sourceTrackerId = sourceTrackerId;
    }

    public String getRawFileName() {
        return rawFileName;
    }

    public String getSourceBucket() {
        return sourceBucket;
    }

    public String getSourcePrefix() {
        return sourcePrefix;
    }

    public String getTargetBucket() {
        return targetBucket;
    }

    /**
     * Gets target prefix. If ingestion status id < 0 i.e. failure, always replace rdz with err.
     *
     * @return - Target prefix.
     */
    public String getTargetPrefix() {
        return eIngestionStatus.ingestionId < 0 ? targetPrefix.replace("rdz", "err") : targetPrefix;
    }

    public Timestamp getLambdaEventTriggerTime() {
        return lambdaEventTriggerTime;
    }

    public Timestamp getRdzWriteTime() {
        return rdzWriteTime;
    }

    public void setRdzWriteTime(Timestamp rdzWriteTime) {
        this.rdzWriteTime = rdzWriteTime;
    }

    public String getStageDBTable() {
        return stageDBTable;
    }

    public void setStageDBTable(String stageDBTable) {
        this.stageDBTable = stageDBTable;
    }

    public GlobalConstant.SOURCE_TYPE getSourceType() {
        return sourceType;
    }

    public Integer getInsertRowCount() {
        return insertRowCount;
    }

    public void setInsertRowCount(Integer insertRowCount) {
        this.insertRowCount = insertRowCount;
    }

    public Timestamp getStageWriteTime() {
        return stageWriteTime;
    }

    public void setStageWriteTime(Timestamp stageWriteTime) {
        this.stageWriteTime = stageWriteTime;
    }

    public Timestamp getWarehouseWriteTime() {
        return warehouseWriteTime;
    }

    public void setWarehouseWriteTime(Timestamp warehouseWriteTime) {
        this.warehouseWriteTime = warehouseWriteTime;
    }

    public void setEIngestionStatus(GlobalConstant.INGESTION_STATUS eIngestionStatus) {
        this.eIngestionStatus = eIngestionStatus;
        // Change the status message when the ingestion status is updated.
    }

    public Integer getIngestionStatusId() {
        return eIngestionStatus.ingestionId;
    }


    public String getStatusMessage() {

        return String.format("%s:%s", eIngestionStatus.statusMessage, statusMessage);
    }

    public void setStatusMessage(String statusMessage) {
        this.statusMessage = statusMessage;
    }
}
