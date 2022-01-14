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

public class S3HelperV2 implements IS3Helper {

    private S3HelperRequest s3HelperRequest;

    public S3HelperV2(S3HelperRequest s3HelperRequest) {
        this.s3HelperRequest = s3HelperRequest;
    }

    private S3Client getS3() {
        System.out.println("inside gets3");
        Region region = Region.US_EAST_1;
        S3Client s3 = S3Client.builder()
                .region(region)
                .build();
        System.out.println("Got S3 client and returning");
        return s3;
    }


    @Override
    public String moveObject() {

        S3Client s3 = getS3();
        GetObjectRequest objectRequest = GetObjectRequest.builder().bucket("mydata-poc").key("sample.json").build();
        CopyObjectRequest copyReq = CopyObjectRequest.builder()
                .sourceBucket(s3HelperRequest.getFromBucket())
                .sourceKey(s3HelperRequest.getFromKey())
                .destinationBucket(s3HelperRequest.getToBucket())
                .destinationKey(s3HelperRequest.getToKey())
                .build();

        try {
            CopyObjectResponse copyRes = s3.copyObject(copyReq);

            System.out.println(copyRes.toString());
            return copyRes.copyObjectResult().toString();
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "";
    }

    @Override
    public void deleteObject() {

    }

    @Override
    public S3HelperResponse readObject() {
        System.out.println("Inside read object");
        S3HelperResponse s3HelperResponse = new S3HelperResponse();
        s3HelperResponse.setOriginalRequest(s3HelperRequest);
        GetObjectRequest req = GetObjectRequest.builder().bucket(s3HelperRequest.getFromBucket()).key(s3HelperRequest.getFromKey()).build();
        System.out.println("object request created");
        ResponseInputStream<GetObjectResponse> s3objectResponse = getS3().getObject(req);
        BufferedReader reader = new BufferedReader(new InputStreamReader(s3objectResponse));

        InputStream targetStream = new ReaderInputStream(reader, Charsets.UTF_8);
        System.out.println("created s3 and got response");
        s3HelperResponse.setObjectStream(targetStream);
        return s3HelperResponse;
    }

    public S3HelperResponse saveFileLocally() {
        System.out.println("Inside DOWNLOAD object");
        S3HelperResponse s3HelperResponse = new S3HelperResponse();
        s3HelperResponse.setOriginalRequest(s3HelperRequest);
        GetObjectRequest req = GetObjectRequest.builder().bucket(s3HelperRequest.getFromBucket()).key(s3HelperRequest.getFromKey()).build();
        System.out.println("DOWNLOAD object request created");
        ResponseInputStream<GetObjectResponse> s3objectResponse = getS3().getObject(req);
        String localFilePath = String.format("/tmp/%s", s3HelperRequest.getFileName());
        System.out.println(String.format("LOCAL FILE NAME: %s", localFilePath));
        try {
            File file = new File(localFilePath);
            IOUtils.copy(s3objectResponse, new FileOutputStream(file));
        } catch (Exception e) {
            e.printStackTrace();
        }
        File localFile = new File(localFilePath);
        if (localFile.exists())
            s3HelperResponse.setLocalFilePath(localFilePath);

        return s3HelperResponse;
    }

}
