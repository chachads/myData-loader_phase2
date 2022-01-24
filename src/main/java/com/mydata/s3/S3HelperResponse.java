package com.mydata.s3;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class S3HelperResponse {
    S3HelperRequest originalRequest;
    InputStream objectStream;
    String localFilePath;
    Boolean hasError;
    List<String> statusMessage = new ArrayList<>();

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

    public Boolean getHasError() {
        return hasError;
    }

    public void setHasError(Boolean hasError) {
        this.hasError = hasError;
    }

    public String getStatusMessage() {
        return Objects.isNull(this.statusMessage) ? "STATUS MESSAGE IS NULL. WHY?" : String.join(":", this.statusMessage);
    }

    public void addStatusMessage(String statusMessage) {
        this.statusMessage.add(statusMessage);
    }
}
