package com.mydata.helper;

import com.mydata.entity.GlobalConstant;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Objects;

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
            SimpleDateFormat sdf1 = new SimpleDateFormat(dateFormat);
            java.util.Date javaDate = sdf1.parse(dateValue);
            return new java.sql.Date(javaDate.getTime());
        } catch (Exception e) {
            return null;
        }
    }

    public static Timestamp getSQLTimestamp(String timestampValue) {
        try {
            String timeStampFormat = "MM/dd/yy hh:mm:ss";
            SimpleDateFormat sdf1 = new SimpleDateFormat(timeStampFormat);
            java.util.Date date = sdf1.parse(timestampValue);
            return new java.sql.Timestamp(date.getTime());
        } catch (ParseException e) {
            return null;
        }
    }

    public static void LogToSystemOut(String message) {
        if (Boolean.parseBoolean(System.getenv(GlobalConstant.ENV_LOG_TO_SYSTEM_OUT)))
            System.out.println(message);
    }

}
