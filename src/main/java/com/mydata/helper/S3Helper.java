package com.mydata.helper;

import com.mydata.entity.S3HelperRequest;
import com.mydata.entity.S3HelperResponse;
import org.apache.commons.codec.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.ReaderInputStream;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.*;
import java.util.Objects;

public class S3Helper implements IS3Helper {

    private S3HelperRequest s3HelperRequest;
    private S3HelperResponse s3HelperResponse;

    public S3Helper(S3HelperRequest s3HelperRequest) {
        this.s3HelperRequest = s3HelperRequest;
        this.s3HelperResponse = new S3HelperResponse();
    }

    private S3Client getS3() {
        CommonUtils.LogToSystemOut("inside gets3");
        Region region = Region.US_EAST_1;
        S3Client s3 = S3Client.builder()
                .region(region)
                .build();
        CommonUtils.LogToSystemOut("Got S3 client and returning");
        return s3;
    }


    @Override
    public S3HelperResponse moveObject() {
        s3HelperResponse.setHasError(false);
        S3Client s3 = getS3();
        CopyObjectRequest copyReq = CopyObjectRequest.builder()
                .sourceBucket(s3HelperRequest.getFromBucket())
                .sourceKey(s3HelperRequest.getFromKey())
                .destinationBucket(s3HelperRequest.getToBucket())
                .destinationKey(s3HelperRequest.getToKey())
                .build();

        try {
            CopyObjectResponse copyRes = s3.copyObject(copyReq);
            s3HelperResponse.addStatusMessage("Copy Object Succeeded.");
            // check if move was successful.
            if (!Objects.isNull(copyRes.copyObjectResult().eTag())) {
                deleteObject();
                s3HelperResponse.addStatusMessage("Delete Object Succeeded.");
            }
        } catch (S3Exception e) {
            s3HelperResponse.setHasError(true);
            s3HelperResponse.addStatusMessage(String.format("copyObject: %s:%s", e.awsErrorDetails().errorMessage(), s3HelperRequest.toString()));
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        CommonUtils.LogToSystemOut("move object finally");
        return s3HelperResponse;
    }

    @Override
    public S3HelperResponse deleteObject() {
        s3HelperResponse.setHasError(false);
        CommonUtils.LogToSystemOut(String.format("Deleting: %s/%s", s3HelperRequest.getFromBucket(), s3HelperRequest.getFromKey()));
        S3Client s3 = getS3();
        DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
                .bucket(s3HelperRequest.getFromBucket())
                .key(s3HelperRequest.getFromKey())
                .build();
        try {
            s3.deleteObject(deleteRequest);
        } catch (S3Exception e) {
            s3HelperResponse.setHasError(true);
            s3HelperResponse.addStatusMessage(String.format("deleteObject: %s:%s", e.awsErrorDetails().errorMessage(), s3HelperRequest.toString()));
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        CommonUtils.LogToSystemOut("delete object finally");
        return s3HelperResponse;
    }

    @Override
    public S3HelperResponse readObject() {
        s3HelperResponse.setHasError(false);
        CommonUtils.LogToSystemOut("Inside read object");
        s3HelperResponse.setOriginalRequest(s3HelperRequest);
        GetObjectRequest req = GetObjectRequest.builder().bucket(s3HelperRequest.getFromBucket()).key(s3HelperRequest.getFromKey()).build();
        CommonUtils.LogToSystemOut("object request created");
        ResponseInputStream<GetObjectResponse> s3objectResponse = getS3().getObject(req);
        BufferedReader reader = new BufferedReader(new InputStreamReader(s3objectResponse));

        InputStream targetStream = new ReaderInputStream(reader, Charsets.UTF_8);
        CommonUtils.LogToSystemOut("created s3 and got response");
        s3HelperResponse.setObjectStream(targetStream);
        return s3HelperResponse;
    }

    public S3HelperResponse saveFileLocally() {
        s3HelperResponse.setHasError(false);
        CommonUtils.LogToSystemOut("Inside DOWNLOAD object");
        s3HelperResponse.setOriginalRequest(s3HelperRequest);
        GetObjectRequest req = GetObjectRequest.builder().bucket(s3HelperRequest.getFromBucket()).key(s3HelperRequest.getFromKey()).build();
        CommonUtils.LogToSystemOut("DOWNLOAD object request created");
        ResponseInputStream<GetObjectResponse> s3objectResponse = getS3().getObject(req);
        String localFilePath = String.format("/tmp/%s", s3HelperRequest.getFileName());
        CommonUtils.LogToSystemOut(String.format("LOCAL FILE NAME: %s", localFilePath));
        try {
            File file = new File(localFilePath);
            IOUtils.copy(s3objectResponse, new FileOutputStream(file));
        } catch (Exception e) {
            e.printStackTrace();
        }
        File localFile = new File(localFilePath);
        if (localFile.exists()) {
            CommonUtils.LogToSystemOut(String.format("local file %s FOUND", localFilePath));
            s3HelperResponse.setLocalFilePath(localFilePath);
        } else
            CommonUtils.LogToSystemOut(String.format("ERROR ERROR ERROR local file %s NOT FOUND", localFilePath));
        return s3HelperResponse;
    }

}
