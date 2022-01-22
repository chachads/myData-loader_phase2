package com.mydata.helper;


import com.mydata.entity.GlobalConstant;
import org.json.JSONObject;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.util.HashMap;

/**
 * To run this AWS code example, ensure that you have setup your development environment, including your AWS credentials.
 * <p>
 * For information, see this documentation topic:
 * <p>
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 */

public class DBConnection {
    public static HashMap<GlobalConstant.DB_CONNECTION_KEY,String> getConnectionDetail(){
        try{
            HashMap<GlobalConstant.DB_CONNECTION_KEY,String> returnMap = new HashMap<>();
            String secretId = System.getenv(GlobalConstant.DB_CONNECTION_KEY.DB_SECRETS_NAME.toString());
            Region region = Region.US_EAST_1;
            SecretsManagerClient secretsClient = SecretsManagerClient.builder()
                    .region(region)
                    .build();
            GetSecretValueRequest valueRequest = GetSecretValueRequest.builder()
                    .secretId(secretId)
                    .build();

            GetSecretValueResponse valueResponse = secretsClient.getSecretValue(valueRequest);
            String secret = valueResponse.secretString();
            CommonUtils.LogToSystemOut(secret);
            JSONObject secretObject = new JSONObject(secret);
            // read proxy host first and then host.
            String hostName = secretObject.toMap().containsKey("proxyhost") ? secretObject.getString("proxyhost"):secretObject.toMap().containsKey("host") ? secretObject.getString("host"):null;
            String uid = secretObject.toMap().containsKey("username") ? secretObject.getString("username") : null;
            String pwd = secretObject.toMap().containsKey("password") ? secretObject.getString("password") : null;
            Integer port = secretObject.toMap().containsKey("port") ? secretObject.getInt("port") : null;
            String dbName = secretObject.toMap().containsKey("dbname") ? secretObject.getString("dbname") : null;

            String connectionString = String.format("jdbc:postgresql://%s:%d/%s",hostName, port, dbName);

            returnMap.putIfAbsent(GlobalConstant.DB_CONNECTION_KEY.DB_CONNECTION_STRING,connectionString);
            returnMap.putIfAbsent(GlobalConstant.DB_CONNECTION_KEY.DB_UID,uid);
            returnMap.putIfAbsent(GlobalConstant.DB_CONNECTION_KEY.DB_PWD,pwd);
            return  returnMap;
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
}
