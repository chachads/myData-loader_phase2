package com.mydata;

import com.mydata.common.GlobalConstant;
import com.mydata.common.CommonUtils;
import org.json.simple.JSONObject;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class App {
    static Connection dbConnection;

    static void establishDBConnection() {
        if (dbConnection == null) {
            try {
                String secretURL = String.format("jdbc:postgresql://%s:%d/%s", "mydata-poc-defvpc.cgzrqpakxtee.us-east-1.rds.amazonaws.com", 5432, "mydata");
                dbConnection = DriverManager.getConnection(secretURL, "postgres", "Memphis0101!");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void debugS3Event(){
        processS3Event e = new processS3Event();
        establishDBConnection();
        String filePath = "/Users/chachads/pocs/do/hg/STAY_Highgate_Hotels_20211217_20211217_1330.json";
        String s3Key = "ldz/ONQ_CRSSTAY/STAY_Highgate_Hotels_20211217_20211217_1330.json";
    }
    public static void main(String[] args) {
        CommonUtils.logToSystemOut("HW");
        String tsValue = "2021-12-15T20:19:10.000Z";
        debugS3Event();
    }

    protected static void dateTest() {
        String arrivalDate = "2021-10-01";
        String departureDate = "2021-10-15";
        java.util.Date beginDate = CommonUtils.getJavaDate(arrivalDate, "yyyy-MM-dd");
        java.util.Date endDate = CommonUtils.getJavaDate(departureDate, "yyyy-MM-dd");
        Long los = TimeUnit.MILLISECONDS.toDays((endDate.getTime() - beginDate.getTime()));
        CommonUtils.logToSystemOut(String.format("LOS: %d", los));
        CommonUtils.logToSystemOut(String.format("%s,%s", beginDate, endDate));
        List<Date> stayDateList = new ArrayList<>();
        for (int i = 0; i < los; i++) {
            stayDateList.add(new Date(CommonUtils.addDays(beginDate, i).getTime()));
        }
        CommonUtils.logToSystemOut("PRINTING DATE RANGE");
        stayDateList.forEach(d -> CommonUtils.logToSystemOut(d.toString()));
    }

    protected static void createPreparedStatement(PreparedStatement preparedStatement, String sourceFormat, Object inputRecord, List<SourceFieldParameter> sourceFieldParameterList, Long etlBatchId, String etlFileName, Integer sourceId, Integer lineNumber) {
        JSONObject jsonRecord = null;
        String[] csvRecord = null;
        switch (sourceFormat) {
            case "CSV":
                if (inputRecord instanceof String[])
                    csvRecord = (String[]) inputRecord;
                break;
            case "JSON":
                if (inputRecord instanceof JSONObject)
                    jsonRecord = (JSONObject) inputRecord;
                break;
        }
        final JSONObject lambdaJSONRecord = jsonRecord;
        final String[] lambdaCSVRecord = csvRecord;

        for (int i = 0; i < sourceFieldParameterList.size(); i++) {
            SourceFieldParameter p = sourceFieldParameterList.get(i);
            GlobalConstant.PSQL_PARAMETER_TYPE fieldType = p.getParameterType();
            try {
                switch (fieldType) {
                    case CHARACTER_VARYING:
                        if (p.getEtlField())
                            if (p.getParameterName().equals(GlobalConstant.ETL_COLUMN_NAME.etl_file_name.toString()))
                                preparedStatement.setString(p.getParameterOrder(), etlFileName);
                            else
                                preparedStatement.setString(p.getParameterOrder(), "");
                        else {
                            switch (sourceFormat) {
                                case "CSV":
                                    preparedStatement.setString(p.getParameterOrder(), (lambdaCSVRecord != null && lambdaCSVRecord.length > Integer.valueOf(p.getParameterOrder()) ? lambdaCSVRecord[p.getParameterOrder() - 1] : null));
                                    break;
                                case "JSON":
                                    preparedStatement.setString(p.getParameterOrder(), (lambdaJSONRecord != null && lambdaJSONRecord.containsKey(p.getParameterName()) && lambdaJSONRecord.get(p.getParameterName()) != null) ? lambdaJSONRecord.get(p.getParameterName()).toString() : null);
                                    break;
                            }
                        }
                        break;
                    case BOOLEAN:
                        boolean fieldBooleanValue;
                        if (p.getEtlField())
                            fieldBooleanValue = false;
                        else {
                            switch (sourceFormat) {
                                case "CSV":
                                    fieldBooleanValue = (lambdaCSVRecord != null && lambdaCSVRecord.length > p.getParameterOrder()) ? CommonUtils.getSQLBoolean(lambdaCSVRecord[p.getParameterOrder() - 1]) : null;
                                    preparedStatement.setBoolean(p.getParameterOrder(), fieldBooleanValue);
                                    break;
                                case "JSON":
                                    fieldBooleanValue = (lambdaJSONRecord != null && lambdaJSONRecord.containsKey(p.getParameterName()) && lambdaJSONRecord.get(p.getParameterName()) != null ? CommonUtils.getSQLBoolean(lambdaJSONRecord.get(p.getParameterName()).toString()) : false);
                                    preparedStatement.setBoolean(p.getParameterOrder(), fieldBooleanValue);
                                    break;
                            }
                        }
                        break;

                    case BIGINT:
                        Long fieldLongValue = null;
                        if (p.getEtlField())
                            if (p.getParameterName().equals(GlobalConstant.ETL_COLUMN_NAME.etl_batch_id.toString()))
                                preparedStatement.setLong(p.getParameterOrder(), etlBatchId);
                        else {
                            switch (sourceFormat) {
                                case "CSV":
                                    fieldLongValue = (lambdaCSVRecord != null && lambdaCSVRecord.length > p.getParameterOrder()) ? Long.parseLong(lambdaCSVRecord[p.getParameterOrder() - 1]) : null;
                                    preparedStatement.setLong(p.getParameterOrder(), fieldLongValue);
                                    break;
                                case "JSON":
                                    fieldLongValue = (lambdaJSONRecord != null && lambdaJSONRecord.containsKey(p.getParameterName()) && lambdaJSONRecord.get(p.getParameterName()) != null ? (Long) lambdaJSONRecord.get(p.getParameterName()) : 0);
                                    preparedStatement.setLong(p.getParameterOrder(), fieldLongValue);
                                    break;
                            }
                        }
                        break;
                    case DOUBLE:
                        Double fieldDoubleValue = 0D;
                        if (p.getEtlField())
                            fieldDoubleValue = null;
                        else {
                            switch (sourceFormat) {
                                case "CSV":
                                    fieldDoubleValue = (lambdaCSVRecord != null && lambdaCSVRecord.length > Integer.valueOf(p.getParameterOrder())) ? Double.parseDouble(lambdaCSVRecord[p.getParameterOrder() - 1]) : null;
                                    preparedStatement.setDouble(p.getParameterOrder(), fieldDoubleValue);
                                    break;
                                case "JSON":
                                    fieldDoubleValue = (lambdaJSONRecord != null && lambdaJSONRecord.containsKey(p.getParameterName()) && lambdaJSONRecord.get(p.getParameterName()) != null ? (Double) lambdaJSONRecord.get(p.getParameterName()) : null);
                                    //            preparedStatement.setDouble(p.getParameterOrder(), fieldDoubleValue);
                                    break;
                            }
                            if (fieldDoubleValue == null)
                                preparedStatement.setNull(p.getParameterOrder(), Types.DOUBLE);
                            else
                                preparedStatement.setDouble(p.getParameterOrder(), fieldDoubleValue);
                        }
                        break;
                    case DATE:
                        Date fieldDateValue = null;
                        if (!p.getEtlField())
                            switch (sourceFormat) {
                                case "CSV":
                                    fieldDateValue = (lambdaCSVRecord != null && lambdaCSVRecord.length > Integer.valueOf(p.getParameterOrder())) ? CommonUtils.getSQLDate(lambdaCSVRecord[p.getParameterOrder() - 1], p.getDateFormat()) : null;
                                    preparedStatement.setDate(p.getParameterOrder(), fieldDateValue);
                                    break;
                                case "JSON":
                                    fieldDateValue = (lambdaJSONRecord != null && lambdaJSONRecord.containsKey(p.getParameterName()) && lambdaJSONRecord.get(p.getParameterName()) != null ? CommonUtils.getSQLDate(lambdaJSONRecord.get(p.getParameterName()).toString(), p.getDateFormat()) : null);
                                    preparedStatement.setDate(p.getParameterOrder(), fieldDateValue);
                                    break;
                            }
                        break;
                    case INTEGER:
                        Integer fieldIntegerValue;
                        if (p.getEtlField()) {
                            fieldIntegerValue = 1;
                            preparedStatement.setInt(p.getParameterOrder(), fieldIntegerValue);
                        } else {
                            if (p.getParameterName().equals("source_id"))
                                preparedStatement.setInt(p.getParameterOrder(), sourceId);
                            else {
                                switch (sourceFormat) {
                                    case "CSV":
                                        fieldIntegerValue = (lambdaCSVRecord != null && lambdaCSVRecord.length > Integer.valueOf(p.getParameterOrder())) ? Integer.parseInt(lambdaCSVRecord[p.getParameterOrder() - 1]) : null;
                                        preparedStatement.setInt(p.getParameterOrder(), fieldIntegerValue);
                                        break;
                                    case "JSON":
                                        fieldIntegerValue = (lambdaJSONRecord != null && lambdaJSONRecord.containsKey(p.getParameterName()) && lambdaJSONRecord.get(p.getParameterName()) != null ? (Integer) lambdaJSONRecord.get(p.getParameterName()) : null);
                                        preparedStatement.setInt(p.getParameterOrder(), fieldIntegerValue);
                                        break;
                                }
                            }
                        }
                        break;
                    case TIMESTAMP:
                        Timestamp fieldTimeStampValue = null;
                        if (p.getEtlField()) {
                            if (p.getParameterName().equals(GlobalConstant.ETL_COLUMN_NAME.etl_ingest_datetime.toString())) {
                                fieldTimeStampValue = new Timestamp(System.currentTimeMillis());
                                preparedStatement.setTimestamp(p.getParameterOrder(), fieldTimeStampValue);

                            }
                        } else {
                            switch (sourceFormat) {
                                case "CSV":
                                    fieldTimeStampValue = (lambdaCSVRecord != null && lambdaCSVRecord.length > p.getParameterOrder()) ? CommonUtils.getSQLTimestamp(lambdaCSVRecord[p.getParameterOrder() - 1], p.getTimestampFormat()) : null;
                                    preparedStatement.setTimestamp(p.getParameterOrder(), fieldTimeStampValue);
                                    break;
                                case "JSON":
                                    fieldTimeStampValue = (lambdaJSONRecord != null && lambdaJSONRecord.containsKey(p.getParameterName()) && lambdaJSONRecord.get(p.getParameterName()) != null ? CommonUtils.getSQLTimestamp(lambdaJSONRecord.get(p.getParameterName()).toString(), p.getTimestampFormat()) : null);
                                    preparedStatement.setTimestamp(p.getParameterOrder(), fieldTimeStampValue);
                                    break;
                            }
                        }
                        break;
                }

            } catch (SQLException e) {
                CommonUtils.logToSystemOut(String.format("Line Number: %d. Column: %s", lineNumber, p));
                e.printStackTrace();
            }
        }
    }

    protected static Boolean getSQLBoolean(String booleanValue) {
        if (booleanValue == null || booleanValue.equals("0") || booleanValue.equalsIgnoreCase("FALSE") || booleanValue.equalsIgnoreCase("N"))
            return false;
        else
            return (booleanValue.equals("1") || booleanValue.equalsIgnoreCase("TRUE") || booleanValue.equalsIgnoreCase("N"));
    }

    protected static Timestamp getSQLTimestamp(String timestampValue) {
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

    protected static Date getSQLDate(String dateValue, String dateFormat) {
        try {
            SimpleDateFormat sdf1 = new SimpleDateFormat(dateFormat);
            java.util.Date javaDate = sdf1.parse(dateValue);
            return new java.sql.Date(javaDate.getTime());
        } catch (Exception e) {
            return null;
        }
    }

    protected static String getParamQueryStringBySource(Integer columnCount) {
        String[] paramList = new String[columnCount];
        Arrays.fill(paramList, "?");
        return String.join(",", paramList);
    }


    // - final methods
    protected static List<SourceFieldParameter> readStageTableDef(IngestSourceDetail ingestSourceDetail) throws SQLException {
        String[] stageTableNameList = ingestSourceDetail.getStageTableName().split("\\.");
        String stageTableName = stageTableNameList[stageTableNameList.length - 1];
        PreparedStatement ps = dbConnection.prepareStatement(String.format("select column_name,data_type,ordinal_position,numeric_precision from information_schema.columns where table_name = '%s' order by ordinal_position;", stageTableName));
        ResultSet rs = ps.executeQuery();
        List<SourceFieldParameter> paramList = new ArrayList<>();
        while (rs.next()) {
            SourceFieldParameter p = new SourceFieldParameter();
            p.setParameterName(rs.getString("column_name"));
            p.setParameterOrder(rs.getInt("ordinal_position"));
            p.setSourceKey(GlobalConstant.SOURCE_KEY.OPERA);
            p.setEtlField(p.getParameterName().startsWith("etl"));
            String paramType = rs.getString("data_type");
            Integer numericPrecision = rs.getInt("numeric_precision");
            switch (paramType) {
                case "character varying":
                    p.setParameterType(GlobalConstant.PSQL_PARAMETER_TYPE.CHARACTER_VARYING);
                    break;
                case "date":
                    p.setParameterType(GlobalConstant.PSQL_PARAMETER_TYPE.DATE);
                    p.setDateFormat(ingestSourceDetail.getSourceDateFormat());
                    break;
                case "timestamp without time zone":
                    p.setParameterType(GlobalConstant.PSQL_PARAMETER_TYPE.TIMESTAMP);
                    p.setTimestampFormat(ingestSourceDetail.getSourceTimeStampFormat());
                    break;
                case "integer":
                    p.setParameterType(GlobalConstant.PSQL_PARAMETER_TYPE.INTEGER);
                    break;
                case "numeric":
                    p.setParameterType(numericPrecision == null ? GlobalConstant.PSQL_PARAMETER_TYPE.INTEGER : GlobalConstant.PSQL_PARAMETER_TYPE.DOUBLE);
                    break;
                case "bigint":
                    p.setParameterType(GlobalConstant.PSQL_PARAMETER_TYPE.BIGINT);
                    break;
                case "boolean":
                    p.setParameterType(GlobalConstant.PSQL_PARAMETER_TYPE.BOOLEAN);
            }
            paramList.add(p);

            //                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "etl_batch_id", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 1, true));
        }
        return paramList;
    }

    public static IngestSourceDetail refreshSourceDefinition(String sourceKey) throws SQLException {
        IngestSourceDetail ingestSourceDetail = new IngestSourceDetail("ldz/ONQ_PMSLEDGER/abcd.csv", "mydata-poc");
        System.out.printf("INSIDE GET SOURCE DETAIL");
        final String sourceDefintionSelectSQL = "SELECT * from lookup.f_lookup_source('%s')";
        PreparedStatement sourceSelection = dbConnection.prepareStatement(String.format(sourceDefintionSelectSQL, sourceKey));
        ResultSet rs = sourceSelection.executeQuery();
        // this should return 1 row.
        if (rs.next()) {
            ingestSourceDetail.setTempTableDefinition(rs.getString("temp_table_definition"));
            ingestSourceDetail.setStageTableName(rs.getString("stage_" +
                    "table_name"));
            ingestSourceDetail.setSourceFormat(rs.getString("source_format"));
            ingestSourceDetail.setDbSourceId(rs.getInt("internal_source_id"));
            ingestSourceDetail.setSourceDateFormat(rs.getString("source_date_format"));
            ingestSourceDetail.setSourceTimeStampFormat(rs.getString("source_timestamp_format"));
            ingestSourceDetail.setWarehouseFunctionName(rs.getString("warehouse_function_name"));
        }
        System.out.printf(ingestSourceDetail.toString());
        return ingestSourceDetail;
    }


}
