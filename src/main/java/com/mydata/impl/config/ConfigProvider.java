package com.mydata.impl.config;

import com.mydata.entity.GlobalConfig;
import com.mydata.entity.S3HelperRequest;
import com.mydata.entity.S3HelperResponse;
import com.mydata.helper.IS3Helper;
import com.mydata.helper.S3HelperV2;

import java.io.IOException;
import java.util.Properties;

public class ConfigProvider implements IConfigProvider {
    @Override
    public GlobalConfig RefreshConfiguration() {
        GlobalConfig globalConfig = new GlobalConfig();
        System.out.println("INSIDE REFRESH CONFIG");
        try {
            refreshProperties(globalConfig);
            refreshIngestSourceDetail(globalConfig);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return globalConfig;
    }

    protected void refreshProperties(GlobalConfig globalConfig) throws IOException {
        System.out.println("Inside refresh properties");
        String configFileBucket = "mydata-poc";
        String configFileKey = "artifacts/mydata.properties";
        Properties envProperties = new Properties();
        S3HelperRequest s3HelperRequest = new S3HelperRequest();
        s3HelperRequest.setFromBucket(configFileBucket);
        s3HelperRequest.setFromKey(configFileKey);
        IS3Helper s3Helper = new S3HelperV2(s3HelperRequest);
        S3HelperResponse s3HelperResponse = s3Helper.readObject();
        System.out.println("back from s3helper");
        envProperties.load(s3HelperResponse.getObjectStream());
/*        if (envProperties.containsKey(GlobalConstant.KEY_DB_URL))
            globalConfig.setDbURL(envProperties.getProperty(GlobalConstant.KEY_DB_URL));
        if (envProperties.containsKey(GlobalConstant.KEY_DB_UID))
            globalConfig.setDbUID(envProperties.getProperty(GlobalConstant.KEY_DB_UID));
        if (envProperties.containsKey(GlobalConstant.KEY_DB_PWD))
            globalConfig.setDbPWD(envProperties.getProperty(GlobalConstant.KEY_DB_PWD));*/
        System.out.println("Finished refresh");
    }

    private void refreshIngestSourceDetail(GlobalConfig globalConfig) {


    }
}

