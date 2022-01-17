package com.mydata.helper;


import com.mydata.entity.DBHelperRequest;
import com.mydata.entity.GlobalConstant;
import org.json.JSONObject;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;

/**
 * To run this AWS code example, ensure that you have setup your development environment, including your AWS credentials.
 * <p>
 * For information, see this documentation topic:
 * <p>
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 */

public class DBConnection {
    public static Connection connectToDB(){
        System.out.println("lambda CONNECTING TO DATABASE");
        try {
            DBHelperRequest dbHelperRequest = System.getenv().containsKey(GlobalConstant.DB_CONNECTION_KEY.DB_SECRETS_NAME.toString())? new DBHelperRequest(System.getenv(GlobalConstant.DB_CONNECTION_KEY.DB_SECRETS_NAME.toString())): new DBHelperRequest();
            System.out.println(String.format("Refreshing db helper request with %s",dbHelperRequest));
            refreshDBHelperRequest(dbHelperRequest);
            return DriverManager.getConnection(dbHelperRequest.getDbURL(), dbHelperRequest.getDbUID(), dbHelperRequest.getDbPWD());
        }
        catch (Exception e){
            System.out.println("Lambda Connection to DB FAILED");
            e.printStackTrace();
        }
        System.out.println("LAMBDA CONNECTED TO DB");
        return  null;
    }
    public static void refreshDBHelperRequest(DBHelperRequest dbHelper) {
        Region region = Region.US_EAST_1;
        SecretsManagerClient secretsClient = SecretsManagerClient.builder()
                .region(region)
                .build();
        try {

            GetSecretValueRequest valueRequest = GetSecretValueRequest.builder()
                    .secretId(dbHelper.getSecretsName())
                    .build();

            GetSecretValueResponse valueResponse = secretsClient.getSecretValue(valueRequest);
            String secret = valueResponse.secretString();
            System.out.println(secret);
            JSONObject secretObject = new JSONObject(secret);
            dbHelper.setDbHost(secretObject.toMap().containsKey("host") ? secretObject.getString("host") : null);
            dbHelper.setDbUID(secretObject.toMap().containsKey("username") ? secretObject.getString("username") : null);
            dbHelper.setDbPWD(secretObject.toMap().containsKey("password") ? secretObject.getString("password") : null);
            dbHelper.setDbPort(secretObject.toMap().containsKey("port") ? secretObject.getInt("port") : null);
            dbHelper.setDbName(secretObject.toMap().containsKey("dbname") ? secretObject.getString("dbname") : null);
            dbHelper.setDbProxyHost(secretObject.toMap().containsKey("proxyhost") ? secretObject.getString("proxyhost") : null);
            dbHelper.setDbSecretsRefreshed(true);
            System.out.println(dbHelper.toString());
        } catch (SecretsManagerException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }

    }


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
            System.out.println(secret);
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
/*
    public static void secretValueWrapper(String secretName) {

        final String USAGE = "" +
                "Usage:" +
                "    <secretName> " +
                "Where:" +
                "    secretName - the name of the secret (for example, tutorials/MyFirstSecret). ";


        Region region = Region.US_EAST_1;
        SecretsManagerClient secretsClient = SecretsManagerClient.builder()
                .region(region)
                .build();

        getValue(secretsClient, secretName);
        secretsClient.close();
    }

    public static void testjsonparse() {
        JSONObject obj = new JSONObject("{interests : [{interestKey:Dogs}, {interestKey:Cats}]}");
        String sm = "{username: postgres,password: Memphis0101!,engine: postgres,host: mydata-postgres-rds.cgzrqpakxtee.us-east-1.rds.amazonaws.com,port: 5432,dbname: mydata,dbInstanceIdentifier: mydata-postgres-rds}";
JSONObject smObj = new JSONObject(sm);
        List<String> list = new ArrayList<String>();
        JSONArray array = obj.getJSONArray("interests");
        for (int i = 0; i < array.length(); i++) {
            list.add(array.getJSONObject(i).getString("interestKey"));
        }
    }

    public static void getValue(SecretsManagerClient secretsClient, String secretName) {

        try {

            GetSecretValueRequest valueRequest = GetSecretValueRequest.builder()
                    .secretId(secretName)
                    .build();

            GetSecretValueResponse valueResponse = secretsClient.getSecretValue(valueRequest);
            String secret = valueResponse.secretString();
            System.out.println(secret);
            JSONObject jo = new JSONObject(secret);
            String secrectValues = String.format("UID: %s, PWD: %s, ENGINE: %s, HOST: %s, PORT: %s, DBNAME: %s, dbIns: %s", jo.getString("username"), jo.getString("password"), jo.getString("engine"), jo.getString("host"), jo.getInt("port"), jo.getString("dbname"), jo.getString("dbInstanceIdentifier"));


            System.out.println(secrectValues);

        } catch (SecretsManagerException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

 */
}
