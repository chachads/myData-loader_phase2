package com.mydata.entity.tracker;

import com.mydata.entity.GlobalConstant;

import java.sql.Timestamp;

public class SourceTrackerDetail {
    private Long fileTrackerId;
    private String rawFileName;
    private String sourceBucket;
    private String sourceKey;
    private String targetBucket;
    private String targetKey;
    private String targetDBTable;
    private Long insertRowCount;
    private Timestamp processStartTime;
    private Timestamp processDBWriteTime;
    private Timestamp processRDZWriteTime;
    private GlobalConstant.SOURCE_TYPE sourceType;

    public SourceTrackerDetail(GlobalConstant.SOURCE_TYPE sourceType, String rawFileName, String sourceBucket, String sourceKey, String targetBucket, String targetKey, String targetDBTable) {
        this.sourceType = sourceType;
        this.rawFileName = rawFileName;
        this.sourceBucket = sourceBucket;
        this.sourceKey = sourceKey;
        this.targetBucket = targetBucket;
        this.targetKey = targetKey;
        this.targetDBTable = targetDBTable;
        this.processStartTime = new Timestamp(System.currentTimeMillis());
    }

    public Long getFileTrackerId() {
        return fileTrackerId;
    }

    public void setFileTrackerId(Long fileTrackerId) {
        this.fileTrackerId = fileTrackerId;
    }

    public String getRawFileName() {
        return rawFileName;
    }

    public String getSourceBucket() {
        return sourceBucket;
    }

    public String getSourceKey() {
        return sourceKey;
    }

    public String getTargetBucket() {
        return targetBucket;
    }

    public String getTargetKey() {
        return targetKey;
    }

    public Timestamp getProcessStartTime() {
        return processStartTime;
    }

    public void setProcessStartTime(Timestamp processStartTime) {
        this.processStartTime = processStartTime;
    }

    public Timestamp getProcessDBWriteTime() {
        return processDBWriteTime;
    }

    public void setProcessDBWriteTime(Timestamp processDBWriteTime) {
        this.processDBWriteTime = processDBWriteTime;
    }

    public Timestamp getProcessRDZWriteTime() {
        return processRDZWriteTime;
    }

    public void setProcessRDZWriteTime(Timestamp processRDZWriteTime) {
        this.processRDZWriteTime = processRDZWriteTime;
    }

    public String getTargetDBTable() {
        return targetDBTable;
    }

    public void setTargetDBTable(String targetDBTable) {
        this.targetDBTable = targetDBTable;
    }

    public GlobalConstant.SOURCE_TYPE getSourceType() {
        return sourceType;
    }

    public void setSourceType(GlobalConstant.SOURCE_TYPE sourceType) {
        this.sourceType = sourceType;
    }

    public Long getInsertRowCount() {
        return insertRowCount;
    }

    public void setInsertRowCount(Long insertRowCount) {
        this.insertRowCount = insertRowCount;
    }
}
