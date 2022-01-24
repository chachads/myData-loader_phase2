package com.mydata.common;

import org.json.JSONObject;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class CommonUtils {
    public static Boolean getSQLBoolean(String booleanValue) {
        if (booleanValue == null || booleanValue.equals("0") || booleanValue.equalsIgnoreCase("FALSE") || booleanValue.equalsIgnoreCase("N"))
            return false;
        else
            return (booleanValue.equals("1") || booleanValue.equalsIgnoreCase("TRUE") || booleanValue.equalsIgnoreCase("N"));
    }
    public static java.util.Date getJavaDate(String dateValue, String inputDateFormat) {
        try {
            SimpleDateFormat sdf1 = new SimpleDateFormat(inputDateFormat);
            return sdf1.parse(dateValue);
        } catch (Exception e) {
            CommonUtils.logErrorToSystemOut(String.format("getJavaDate: %s", e.getMessage()));
            return null;
        }
    }
    public static String getDateStr(String outputFormat, java.util.Date inputDate) {
        DateFormat dateFormat = new SimpleDateFormat(outputFormat);
        if (Objects.isNull(inputDate))
            inputDate = Calendar.getInstance().getTime();
        return dateFormat.format(inputDate);
    }
    public static java.util.Date addDays(java.util.Date inputDate, Integer numberOfDays) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(inputDate);
        calendar.add(Calendar.DAY_OF_MONTH, numberOfDays);
        return calendar.getTime();
    }
    public static Date getSQLDate(String dateValue, String dateFormat) {
        try {
            if (Objects.isNull(dateValue) || dateValue.trim().equalsIgnoreCase("NULL"))
                return null;
            else {
                SimpleDateFormat sdf1 = new SimpleDateFormat(dateFormat);
                java.util.Date javaDate = sdf1.parse(dateValue);
                return new java.sql.Date(javaDate.getTime());
            }
        } catch (Exception e) {
            CommonUtils.logErrorToSystemOut(String.format("ERROR: getSQLDate: %s", e.getMessage()));
            return null;
        }
    }
    public static Timestamp getSQLTimestamp(String timestampValue, String timestampFormat) {
        try {
            if (Objects.isNull(timestampValue) || timestampValue.trim().equalsIgnoreCase("NULL"))
                return null;
            else {
                SimpleDateFormat format = new SimpleDateFormat(timestampFormat, Locale.US);
                format.setTimeZone(TimeZone.getTimeZone("UTC"));
                java.util.Date date = format.parse(timestampValue);
                return new java.sql.Timestamp(date.getTime());
            }
        } catch (ParseException e) {
            CommonUtils.logErrorToSystemOut(String.format("ERROR: getSQLTimestamp: %s", e.getMessage()));
            return null;
        }
    }
    public static void logErrorToSystemOut(String message) {
        System.out.println(message);
    }
    public static void logToSystemOut(String message) {
        if (Boolean.parseBoolean(System.getenv(GlobalConstant.ENV_LOG_TO_SYSTEM_OUT)))
            System.out.println(message);
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
            CommonUtils.logErrorToSystemOut(e.getMessage());
        }
        return null;
    }
}
