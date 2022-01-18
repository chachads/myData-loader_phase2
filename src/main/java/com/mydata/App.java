package com.mydata;

import com.mydata.entity.GlobalConstant;
import com.mydata.entity.SourceFieldParameter;
import com.mydata.entity.domain.IngestSourceDetail;
import com.mydata.helper.CommonUtils;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Date;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
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

    public static void main(String[] args) {
        System.out.println("HW");
        String arrivalDate = "2021-10-01";
        String departureDate = "2021-10-15";
        java.util.Date beginDate = CommonUtils.getJavaDate(arrivalDate, "yyyy-MM-dd");
        java.util.Date endDate = CommonUtils.getJavaDate(departureDate, "yyyy-MM-dd");
        Long los = TimeUnit.MILLISECONDS.toDays((endDate.getTime() - beginDate.getTime()));
        System.out.println(String.format("LOS: %d", los));
        System.out.println(String.format("%s,%s", beginDate, endDate));
        List<Date> stayDateList = new ArrayList<>();
        for (int i = 0; i < los; i++) {
            stayDateList.add(new Date(CommonUtils.addDays(beginDate,i).getTime()));
        }
        System.out.println("PRINTING DATE RANGE");
        stayDateList.forEach(d -> System.out.println(d));

    }


    protected static void loadData() {

        try {
            String sourceFormat = "CSV";
            establishDBConnection();
            IngestSourceDetail ingestSourceDetail = refreshSourceDefinition("ONQ_PMSLEDGER");
            List<SourceFieldParameter> paramList = readStageTableDef(ingestSourceDetail);
            String paramQueryString = getParamQueryStringBySource(paramList.size());
            String etlBatchId = "t" + UUID.randomUUID().toString().replace("-", "");
            //String fileFullName = "/Users/chachads/Downloads/GLAPX_20211018_OTB.csv";
            String fileFullName = "/Users/chachads/Downloads/LEDGER_Highgate_Hotels_20211214_20211215_1230.json";
            File file = new File(fileFullName);
            String etlFileName = (file.exists()) ? file.getName() : "notfound";
            String stageTableName = ingestSourceDetail.getStageTableName();
            ingestSourceDetail.setLocalFilePath(fileFullName);
            Long rowCount = 0L;
            Integer batchSize = 1000;
            PreparedStatement preparedStatement = dbConnection.prepareStatement(String.format("INSERT INTO %s values (%s)", stageTableName, paramQueryString));

            switch (ingestSourceDetail.getSourceFormat()) {
                case "CSV":
                    BufferedReader reader = new BufferedReader(new FileReader(ingestSourceDetail.getLocalFilePath()));
                    CSVParser parser = new CSVParserBuilder().withSeparator(',').withIgnoreQuotations(true).build();
                    CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(1).withCSVParser(parser).build();
                    String[] line;
                    Boolean openBatch = false;
                    while ((line = csvReader.readNext()) != null) {
                        rowCount++;
                        openBatch = true;
                        createPreparedStatement(preparedStatement, ingestSourceDetail.getSourceFormat(), line, paramList, etlBatchId, etlFileName, ingestSourceDetail.getDbSourceId());
                        preparedStatement.addBatch();
                        if (rowCount % batchSize == 0) {
                            System.out.println(String.format("COMMITTING BATCH. Row Count: %d", rowCount));
                            int[] rowsInserted = preparedStatement.executeBatch();
                            System.out.println(String.format("COMMITTING BATCH. Row Count: %d. Execute Status: %d", rowCount, rowsInserted.length));
                            openBatch = false;
                        }
                    }
                    if (openBatch) {
                        int[] rowsInserted = preparedStatement.executeBatch();
                        System.out.println(String.format("COMMITTING BATCH. Row Count: %d. Execute Status: %d", rowCount, rowsInserted.length));
                    }
                    break;
                case "JSON":

                    JSONParser jsonParser = new JSONParser();
                    JSONArray jarray = (JSONArray) jsonParser.parse(new FileReader(ingestSourceDetail.getLocalFilePath()));
                    for (Object object : jarray) {
                        rowCount++;
                        createPreparedStatement(preparedStatement, ingestSourceDetail.getSourceFormat(), object, paramList, etlBatchId, etlFileName, ingestSourceDetail.getDbSourceId());

                        preparedStatement.addBatch();
                        if (rowCount % batchSize == 0 || rowCount == jarray.size()) {
                            System.out.println(String.format("COMMITTING BATCH. Row Count: %d", rowCount));
                            int[] rowsInserted = preparedStatement.executeBatch();
                            System.out.println(String.format("COMMITTING BATCH. Row Count: %d. Execute Status: %d", rowCount, rowsInserted.length));
                        }
                        ingestSourceDetail.getFileTrackerDetail().setInsertRowCount(rowCount);
                    }

                    System.out.println("finished parsing");
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    protected static void createPreparedStatement(PreparedStatement preparedStatement, String sourceFormat, Object inputRecord, List<SourceFieldParameter> sourceFieldParameterList, String etlBatchId, String etlFileName, Integer sourceId) {
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
        sourceFieldParameterList.forEach(p -> {
            GlobalConstant.PSQL_PARAMETER_TYPE fieldType = p.getParameterType();
            try {
                switch (fieldType) {
                    case CHARACTER_VARYING:
                        if (p.getEtlField())
                            if (p.getParameterName().equals(GlobalConstant.ETL_COLUMN_NAME.etl_batch_id.toString()))
                                preparedStatement.setString(p.getParameterOrder(), etlBatchId);
                            else if (p.getParameterName().equals(GlobalConstant.ETL_COLUMN_NAME.etl_file_name.toString()))
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
                        boolean fieldBooleanValue = false;
                        if (p.getEtlField())
                            fieldBooleanValue = false;
                        else {
                            switch (sourceFormat) {
                                case "CSV":
                                    fieldBooleanValue = (lambdaCSVRecord != null && lambdaCSVRecord.length > Integer.valueOf(p.getParameterOrder())) ? getSQLBoolean(lambdaCSVRecord[p.getParameterOrder() - 1]) : null;
                                    preparedStatement.setBoolean(p.getParameterOrder(), fieldBooleanValue);
                                    break;
                                case "JSON":
                                    fieldBooleanValue = (lambdaJSONRecord != null && lambdaJSONRecord.containsKey(p.getParameterName()) && lambdaJSONRecord.get(p.getParameterName()) != null ? getSQLBoolean(lambdaJSONRecord.get(p.getParameterName()).toString()) : false);
                                    preparedStatement.setBoolean(p.getParameterOrder(), fieldBooleanValue);
                                    break;
                            }
                        }
                        break;

                    case BIGINT:
                        Long fieldLongValue = null;
                        if (p.getEtlField())
                            fieldLongValue = null;
                        else {
                            switch (sourceFormat) {
                                case "CSV":
                                    fieldLongValue = (lambdaCSVRecord != null && lambdaCSVRecord.length > Integer.valueOf(p.getParameterOrder())) ? Long.parseLong(lambdaCSVRecord[p.getParameterOrder() - 1]) : null;
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
                        Double fieldDoubleValue = null;
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
                                    preparedStatement.setDouble(p.getParameterOrder(), fieldDoubleValue);
                                    break;
                            }
                        }
                        break;
                    case DATE:
                        Date fieldDateValue = null;
                        if (!p.getEtlField())
                            switch (sourceFormat) {
                                case "CSV":
                                    fieldDateValue = (lambdaCSVRecord != null && lambdaCSVRecord.length > Integer.valueOf(p.getParameterOrder())) ? getSQLDate(lambdaCSVRecord[p.getParameterOrder() - 1], p.getDateFormat()) : null;
                                    preparedStatement.setDate(p.getParameterOrder(), fieldDateValue);
                                    break;
                                case "JSON":
                                    fieldDateValue = (lambdaJSONRecord != null && lambdaJSONRecord.containsKey(p.getParameterName()) && lambdaJSONRecord.get(p.getParameterName()) != null ? getSQLDate(lambdaJSONRecord.get(p.getParameterName()).toString(), p.getDateFormat()) : null);
                                    preparedStatement.setDate(p.getParameterOrder(), fieldDateValue);
                                    break;
                            }
                        break;
                    case INTEGER:
                        Integer fieldIntegerValue = null;
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
                                    fieldTimeStampValue = (lambdaCSVRecord != null && lambdaCSVRecord.length > Integer.valueOf(p.getParameterOrder())) ? getSQLTimestamp(lambdaCSVRecord[p.getParameterOrder() - 1]) : null;
                                    preparedStatement.setTimestamp(p.getParameterOrder(), fieldTimeStampValue);
                                    break;
                                case "JSON":
                                    fieldTimeStampValue = (lambdaJSONRecord != null && lambdaJSONRecord.containsKey(p.getParameterName()) && lambdaJSONRecord.get(p.getParameterName()) != null ? getSQLTimestamp(lambdaJSONRecord.get(p.getParameterName()).toString()) : null);
                                    preparedStatement.setTimestamp(p.getParameterOrder(), fieldTimeStampValue);
                                    break;
                            }
                        }
                        break;
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        //      System.out.println(preparedStatement.toString());

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
        }
        System.out.printf(ingestSourceDetail.toString());
        return ingestSourceDetail;
    }


}
