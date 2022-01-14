package com.mydata.impl.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.mydata.entity.domain.IngestSourceDetail;
import com.mydata.impl.IngestionWorker;

import java.sql.Connection;

public class processS3Event implements RequestHandler<S3Event, Object> {
    static final String DB_ENDPOINT = "mydata-rds-postgres-proxy.proxy-cgzrqpakxtee.us-east-1.rds.amazonaws.com";
    static final String DB_REGION = "us-east-1";// System.getenv("us-east-1");
    private Connection con = null;
    private String dbRdsProxyUser;
    private String dbRdsProxyUserPwd;

    public processS3Event() {
/*
        // Admin credentials for RDS instance.

        String envdbAdminSecret = "mydata-postgres-rds-sm";

        System.out.println("In S3 handler counstructor");
        System.out.println(String.format("DB Endpoint: %s. DB_Region: %s", DB_ENDPOINT, DB_REGION));
        JSONObject dbAdminSecret = SecretsUtil.getSecret(DB_REGION, envdbAdminSecret);
        String dbAdminUser = (String) dbAdminSecret.get("username");
        String dbAdminPwd = (String) dbAdminSecret.get("password");

        System.out.println(String.format("DBAdmin User: %s. DBAdmin PWD: %s", dbAdminUser, dbAdminPwd));

        JSONObject dbUserSecret = SecretsUtil.getSecret(DB_REGION, System.getenv("DB_USER_SECRET"));
        this.dbRdsProxyUser = (String) dbUserSecret.get("username");
        this.dbRdsProxyUserPwd = (String) dbUserSecret.get("password");

        try {
            this.con = DbUtil.createConnectionViaUserPwd(dbAdminUser, dbAdminPwd, DB_ENDPOINT);

        } catch (Exception e) {
            System.out.println("INIT connection FAILED");
            System.out.println(e.getMessage());
        }

        System.out.println("PopulateFarmDb empty constructor, called by AWS Lambda");
*/
    }

    public Object handleRequest(S3Event s3Event, Context context) {
        String s3Bucket = s3Event.getRecords().get(0).getS3().getBucket().getName();
        String s3Key = s3Event.getRecords().get(0).getS3().getObject().getKey();
        System.out.println(String.format("Bucket: %s. Key: %s", s3Bucket, s3Key));
        IngestSourceDetail ingestSourceDetail = new IngestSourceDetail(s3Key, s3Bucket);
        System.out.println(ingestSourceDetail.toString());
        IngestionWorker worker = new IngestionWorker(ingestSourceDetail);
        worker.doWork();
/*
        IS3Helper s3 = new S3HelperV2(ingestSourceDetail.getS3HelperRequest());
        System.out.println("CALLING SAVE FILE LOCALLY");
        s3.saveFileLocally();
        InputStream streamToLoad = s3.readObject().getObjectStream();
        System.out.println("END - READING INPUT STREAM");
        System.out.println("START - WRITING STREAM TO DB");

        ingestSourceDetail.getDbHelper().loadStream(ingestSourceDetail, streamToLoad);*/
        System.out.println("END - WRITING STREAM TO DB");
        return null;
    }


}
