package com.mydata.impl;

import com.mydata.entity.S3HelperResponse;
import com.mydata.entity.domain.IngestSourceDetail;
import com.mydata.helper.IS3Helper;
import com.mydata.helper.S3HelperV2;

import java.io.InputStream;
import java.sql.Timestamp;

public class IngestionWorker {
    private final IngestSourceDetail ingestSourceDetail;

    public IngestionWorker(IngestSourceDetail ingestSourceDetail) {
        this.ingestSourceDetail = ingestSourceDetail;
    }


    public void doWork(Boolean newWorker){

    }
    public void doWork() {
        IS3Helper s3 = new S3HelperV2(ingestSourceDetail.getS3HelperRequest());
        s3.moveObject();
        //IDBHelper dbHelper = new DBHelper(new DBHelperRequest());
        switch (ingestSourceDetail.getSourceFormat()) {
            case "CSV":
                System.out.println("CSV CASE");
                InputStream streamToLoad = s3.readObject().getObjectStream();
                ingestSourceDetail.getDbHelper().loadStream(ingestSourceDetail, streamToLoad);
                break;
            case "JSON":
                System.out.println("JSON CASE");
                S3HelperResponse s3HelperResponse = s3.saveFileLocally();
                ingestSourceDetail.setLocalFilePath(s3HelperResponse.getLocalFilePath());
                ingestSourceDetail.getDbHelper().readAndLoadJSON(ingestSourceDetail);
                break;
        }
        ingestSourceDetail.getFileTrackerDetail().setProcessDBWriteTime(new Timestamp(System.currentTimeMillis()));
        ingestSourceDetail.getDbHelper().saveFileTracker(ingestSourceDetail.getFileTrackerDetail());
    }



}
