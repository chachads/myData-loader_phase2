package com.mydata.entity.tracker;

import com.mydata.entity.GlobalConstant;

import java.sql.Timestamp;

public class IngestionTrackerDetail {
    private Long sourceTrackerId;
    private String sourceKey;
    private String rawFileName;
    private String sourceBucket;
    private String sourcePrefix;
    private String targetBucket;
    private String targetPrefix;
    private String stageDBTable;
    private Integer insertRowCount;
    private Timestamp lambdaEventTriggerTime;
    private Timestamp stageWriteTime;
    private Timestamp warehouseWriteTime;
    private Timestamp rdzWriteTime;
    private GlobalConstant.SOURCE_TYPE sourceType;
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

    public String getTargetPrefix() {
        return targetPrefix;
    }

    public Timestamp getLambdaEventTriggerTime() {
        return lambdaEventTriggerTime;
    }

    public void setLambdaEventTriggerTime(Timestamp lambdaEventTriggerTime) {
        this.lambdaEventTriggerTime = lambdaEventTriggerTime;
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

    public void setSourceType(GlobalConstant.SOURCE_TYPE sourceType) {
        this.sourceType = sourceType;
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

    public GlobalConstant.INGESTION_STATUS geteIngestionStatus() {
        return eIngestionStatus;
    }

    public void seteIngestionStatus(GlobalConstant.INGESTION_STATUS eIngestionStatus) {
        this.eIngestionStatus = eIngestionStatus;
    }

    public Integer getIngestionStatus() {
        return eIngestionStatus.getValue();
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public void setStatusMessage(String statusMessage) {
        this.statusMessage = statusMessage;
    }
}
