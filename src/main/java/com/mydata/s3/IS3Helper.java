package com.mydata.s3;

public interface IS3Helper {
    S3HelperResponse moveObject();

    S3HelperResponse deleteObject();

    S3HelperResponse readObject();

    S3HelperResponse saveFileLocally();
}
