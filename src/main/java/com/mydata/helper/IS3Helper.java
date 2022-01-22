package com.mydata.helper;

import com.mydata.entity.S3HelperResponse;

public interface IS3Helper {
    S3HelperResponse moveObject();

    S3HelperResponse deleteObject();

    S3HelperResponse readObject();

    S3HelperResponse saveFileLocally();
}
