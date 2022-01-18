package com.mydata.helper;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class CommonUtils {
    public static Boolean getSQLBoolean(String booleanValue) {
        if (booleanValue == null || booleanValue.equals("0") || booleanValue.equalsIgnoreCase("FALSE") || booleanValue.equalsIgnoreCase("N"))
            return false;
        else
            return (booleanValue.equals("1") || booleanValue.equalsIgnoreCase("TRUE") || booleanValue.equalsIgnoreCase("N"));
    }
    public static java.util.Date getJavaDate(String dateValue, String dateFormat) {
        try {
            SimpleDateFormat sdf1 = new SimpleDateFormat(dateFormat);
            return sdf1.parse(dateValue);
        } catch (Exception e) {
            return null;
        }
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
            java.sql.Timestamp sqlStartDate = new java.sql.Timestamp(date.getTime());
            return sqlStartDate;
        } catch (ParseException e) {
            return null;
        }
    }

}
