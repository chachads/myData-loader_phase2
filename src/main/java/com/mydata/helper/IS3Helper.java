package com.mydata.helper;

import com.mydata.entity.S3HelperResponse;

public interface IS3Helper {
    String moveObject();

    void deleteObject();

    S3HelperResponse readObject();

    S3HelperResponse saveFileLocally();
}
