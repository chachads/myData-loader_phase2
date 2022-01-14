package com.mydata.entity;

import java.io.InputStream;

public class S3HelperResponse {
    S3HelperRequest originalRequest;
    InputStream objectStream;
    String localFilePath;

    public S3HelperRequest getOriginalRequest() {
        return originalRequest;
    }

    public void setOriginalRequest(S3HelperRequest originalRequest) {
        this.originalRequest = originalRequest;
    }

    public InputStream getObjectStream() {
        return objectStream;
    }

    public void setObjectStream(InputStream objectStream) {
        this.objectStream = objectStream;
    }

    public String getLocalFilePath() {
        return localFilePath;
    }

    public void setLocalFilePath(String localFilePath) {
        this.localFilePath = localFilePath;
    }
}
