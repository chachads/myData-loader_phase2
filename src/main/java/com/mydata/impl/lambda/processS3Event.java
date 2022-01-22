package com.mydata.impl.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.mydata.entity.GlobalConstant;
import com.mydata.entity.S3HelperResponse;
import com.mydata.entity.SourceFieldParameter;
import com.mydata.entity.domain.IngestSourceDetail;
import com.mydata.entity.tracker.IngestionTrackerDetail;
import com.mydata.helper.CommonUtils;
import com.mydata.helper.DBConnection;
import com.mydata.helper.IS3Helper;
import com.mydata.helper.S3Helper;
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
import java.util.*;

public class processS3Event implements RequestHandler<S3Event, Object> {

    static Connection dbConnection;

    public Object handleRequest(S3Event s3Event, Context context) {
        String s3Bucket = s3Event.getRecords().get(0).getS3().getBucket().getName();
        String s3Key = s3Event.getRecords().get(0).getS3().getObject().getKey();
        CommonUtils.LogToSystemOut(String.format("Bucket: %s. Key: %s", s3Bucket, s3Key));
        IngestSourceDetail ingestSourceDetail = new IngestSourceDetail(s3Key, s3Bucket);
        try {
            if (Objects.isNull(dbConnection)) {
                try {
                    HashMap<GlobalConstant.DB_CONNECTION_KEY, String> connectionDetail = DBConnection.getConnectionDetail();
                    if (!Objects.isNull(connectionDetail))
                        dbConnection = DriverManager.getConnection(connectionDetail.get(GlobalConstant.DB_CONNECTION_KEY.DB_CONNECTION_STRING), connectionDetail.get(GlobalConstant.DB_CONNECTION_KEY.DB_UID), connectionDetail.get(GlobalConstant.DB_CONNECTION_KEY.DB_PWD));
                    else
                        dbConnection = null;
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
            // log source tracker for s3 trigger event.
            logSourceTrackerIU(ingestSourceDetail.getIngestionTrackerDetail());
            refreshSourceDefinition(ingestSourceDetail);
            List<SourceFieldParameter> paramList = readStageTableDef(ingestSourceDetail);
            String paramQueryString = getParamQueryStringBySource(paramList.size());

            Integer rowCount = 0;
            Integer batchSize = 1000;

            PreparedStatement preparedStatement = dbConnection.prepareStatement(String.format("INSERT INTO %s values (%s)", ingestSourceDetail.getStageTableName(), paramQueryString));

            IS3Helper s3 = new S3Helper(ingestSourceDetail.getS3HelperRequest());
            S3HelperResponse s3HelperResponse = s3.saveFileLocally();
            String etlBatchId = "t" + UUID.randomUUID().toString().replace("-", "");
            File localFile = new File(s3HelperResponse.getLocalFilePath());
            String etlFileName = localFile.exists() ? localFile.getName() : "error";
            CommonUtils.LogToSystemOut(String.format("ETL FILE NAME IS %s", etlFileName));
            switch (ingestSourceDetail.getSourceFormat()) {
                case "CSV":
                    BufferedReader reader = new BufferedReader(new FileReader(s3HelperResponse.getLocalFilePath()));
                    CSVParser parser = new CSVParserBuilder().withSeparator(',').withIgnoreQuotations(true).build();
                    CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(1).withCSVParser(parser).build();
                    String[] line;
                    boolean openBatch = false;
                    while ((line = csvReader.readNext()) != null) {
                        rowCount++;
                        openBatch = true;
                        createPreparedStatement(preparedStatement, ingestSourceDetail.getSourceFormat(), line, paramList, etlBatchId, etlFileName, ingestSourceDetail.getDbSourceId(), rowCount);
                        preparedStatement.addBatch();
                        if (rowCount % batchSize == 0) {
                            CommonUtils.LogToSystemOut(String.format("COMMITTING BATCH. Row Count: %d", rowCount));
                            int[] rowsInserted = preparedStatement.executeBatch();
                            CommonUtils.LogToSystemOut(String.format("COMMITTING BATCH. Row Count: %d. Execute Status: %d", rowCount, rowsInserted.length));
                            openBatch = false;
                        }
                    }
                    if (openBatch) {
                        int[] rowsInserted = preparedStatement.executeBatch();
                        CommonUtils.LogToSystemOut(String.format("COMMITTING BATCH. Row Count: %d. Execute Status: %d", rowCount, rowsInserted.length));
                    }
                    //update stage complete status
                    ingestSourceDetail.getIngestionTrackerDetail().seteIngestionStatus(GlobalConstant.INGESTION_STATUS.STAGE_WRITE_COMPLETE);
                    ingestSourceDetail.getIngestionTrackerDetail().setStageWriteTime(new Timestamp(System.currentTimeMillis()));
                    ingestSourceDetail.getIngestionTrackerDetail().setInsertRowCount(rowCount);
                    logSourceTrackerIU(ingestSourceDetail.getIngestionTrackerDetail());
                    CommonUtils.LogToSystemOut("MOVING TO STAGE");
                    PreparedStatement moveToStage = dbConnection.prepareStatement(String.format("select * from lookup.f_process_stage_opera('%s');", etlBatchId));
                    CommonUtils.LogToSystemOut(moveToStage.toString());
                    moveToStage.execute();
                    // update move to warehouse completed
                    ingestSourceDetail.getIngestionTrackerDetail().seteIngestionStatus(GlobalConstant.INGESTION_STATUS.WAREHOUSE_WRITE_COMPLETE);
                    ingestSourceDetail.getIngestionTrackerDetail().setWarehouseWriteTime(new Timestamp(System.currentTimeMillis()));
                    logSourceTrackerIU(ingestSourceDetail.getIngestionTrackerDetail());
                    CommonUtils.LogToSystemOut("closing readers");
                    reader.close();
                    csvReader.close();
                    break;
                case "JSON":
                    JSONParser jsonParser = new JSONParser();
                    JSONArray jarray = (JSONArray) jsonParser.parse(new FileReader(s3HelperResponse.getLocalFilePath()));
                    CommonUtils.LogToSystemOut(String.format("JSON ARRAY PARSED FROM %s with size %d", s3HelperResponse.getLocalFilePath(), jarray.size()));
                    for (Object object : jarray) {
                        rowCount++;
                        createPreparedStatement(preparedStatement, ingestSourceDetail.getSourceFormat(), object, paramList, etlBatchId, etlFileName, ingestSourceDetail.getDbSourceId(), rowCount);
                        preparedStatement.addBatch();
                        if (rowCount % batchSize == 0 || rowCount == jarray.size()) {
                            CommonUtils.LogToSystemOut(String.format("COMMITTING BATCH. Row Count: %d", rowCount));
                            int[] rowsInserted = preparedStatement.executeBatch();
                            CommonUtils.LogToSystemOut(String.format("COMMITTING BATCH. Row Count: %d. Execute Status: %d", rowCount, rowsInserted.length));
                        }
                    }
                    //update stage complete status
                    ingestSourceDetail.getIngestionTrackerDetail().seteIngestionStatus(GlobalConstant.INGESTION_STATUS.STAGE_WRITE_COMPLETE);
                    ingestSourceDetail.getIngestionTrackerDetail().setStageWriteTime(new Timestamp(System.currentTimeMillis()));
                    ingestSourceDetail.getIngestionTrackerDetail().setInsertRowCount(rowCount);
                    logSourceTrackerIU(ingestSourceDetail.getIngestionTrackerDetail());
                    break;
            }
            S3HelperResponse moveObjectResponse = s3.moveObject();
            CommonUtils.LogToSystemOut(String.format("back from S3 MOVE OBJECT. %s. Status Message: %s",moveObjectResponse.getHasError(),moveObjectResponse.getStatusMessage()));
            if (moveObjectResponse.getHasError()) {
                ingestSourceDetail.getIngestionTrackerDetail().seteIngestionStatus(GlobalConstant.INGESTION_STATUS.RDZ_WRITE_FAILED);
            } else {
                ingestSourceDetail.getIngestionTrackerDetail().setRdzWriteTime(new Timestamp(System.currentTimeMillis()));
                ingestSourceDetail.getIngestionTrackerDetail().seteIngestionStatus(GlobalConstant.INGESTION_STATUS.RDZ_WRITE_COMPLETE);
            }
            ingestSourceDetail.setStatusMessage(moveObjectResponse.getStatusMessage());
            ingestSourceDetail.getIngestionTrackerDetail().setStatusMessage(moveObjectResponse.getStatusMessage());
            CommonUtils.LogToSystemOut("going to log source tracker");
            logSourceTrackerIU(ingestSourceDetail.getIngestionTrackerDetail());
            // Now mark entire request as completed
            ingestSourceDetail.setStatusMessage("Ingestion Complete");
            ingestSourceDetail.getIngestionTrackerDetail().setStatusMessage("Ingestion Complete");
            ingestSourceDetail.getIngestionTrackerDetail().seteIngestionStatus(GlobalConstant.INGESTION_STATUS.INGESTION_COMPLETE);
            logSourceTrackerIU(ingestSourceDetail.getIngestionTrackerDetail());
            CommonUtils.LogToSystemOut("END - WRITING STREAM TO DB");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            logSourceFileLookup(ingestSourceDetail);
        }
        return null;
    }


    public static void refreshSourceDefinition(IngestSourceDetail ingestSourceDetail) throws SQLException {
        CommonUtils.LogToSystemOut("INSIDE GET SOURCE DETAIL");
        final String sourceDefintionSelectSQL = "SELECT * from lookup.f_lookup_source('%s')";
        PreparedStatement sourceSelection = dbConnection.prepareStatement(String.format(sourceDefintionSelectSQL, ingestSourceDetail.getSourceKey()));
        ResultSet rs = sourceSelection.executeQuery();
        // this should return 1 row.
        if (rs.next()) {
            ingestSourceDetail.setTempTableDefinition(rs.getString("temp_table_definition"));
            ingestSourceDetail.setStageTableName(rs.getString("stage_table_name"));
            ingestSourceDetail.getIngestionTrackerDetail().setStageDBTable(ingestSourceDetail.getStageTableName());
            ingestSourceDetail.setSourceFormat(rs.getString("source_format"));
            ingestSourceDetail.setDbSourceId(rs.getInt("internal_source_id"));
            ingestSourceDetail.setSourceDateFormat(rs.getString("source_date_format"));
        }
        CommonUtils.LogToSystemOut(ingestSourceDetail.toString());
    }

    protected static List<SourceFieldParameter> readStageTableDef(IngestSourceDetail ingestSourceDetail) throws SQLException {
        String[] stageTableNameList = ingestSourceDetail.getStageTableName().split("\\.");
        String stageTableName = stageTableNameList[stageTableNameList.length - 1];
        PreparedStatement ps = getTableSchemaDefinition(stageTableName);
        ResultSet rs = ps.executeQuery();
        List<SourceFieldParameter> paramList = new ArrayList<>();
        while (rs.next()) {
            SourceFieldParameter p = new SourceFieldParameter();
            p.setParameterName(rs.getString("column_name"));
            p.setParameterOrder(rs.getInt("ordinal_position"));
            p.setSourceKey(ingestSourceDetail.getESourceKey());
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

    protected static PreparedStatement getTableSchemaDefinition(String tableName) throws SQLException {
        return dbConnection.prepareStatement(String.format("select column_name,data_type,ordinal_position,numeric_precision from information_schema.columns where table_name = '%s' order by ordinal_position;", tableName));

    }

    protected static String getParamQueryStringBySource(Integer columnCount) {
        String[] paramList = new String[columnCount];
        Arrays.fill(paramList, "?");
        return String.join(",", paramList);
    }


    protected static void createPreparedStatement(PreparedStatement preparedStatement, String sourceFormat, Object inputRecord, List<SourceFieldParameter> sourceFieldParameterList, String etlBatchId, String etlFileName, Integer sourceId, Integer lineNumber) {
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
                        String fieldStringValue;
                        if (p.getEtlField())
                            if (p.getParameterName().equals(GlobalConstant.ETL_COLUMN_NAME.etl_batch_id.toString()))
                                fieldStringValue = etlBatchId;
                            else if (p.getParameterName().equals(GlobalConstant.ETL_COLUMN_NAME.etl_file_name.toString()))
                                fieldStringValue = etlFileName;
                            else
                                fieldStringValue = null;
                        else {
                            switch (sourceFormat) {
                                case "CSV":
                                    fieldStringValue = lambdaCSVRecord != null && lambdaCSVRecord.length > p.getParameterOrder() ? lambdaCSVRecord[p.getParameterOrder() - 1] : null;
                                    break;
                                case "JSON":
                                    fieldStringValue = (lambdaJSONRecord != null && lambdaJSONRecord.containsKey(p.getParameterName()) && lambdaJSONRecord.get(p.getParameterName()) != null) ? lambdaJSONRecord.get(p.getParameterName()).toString() : null;
                                    break;
                                default:
                                    fieldStringValue = null;
                                    break;
                            }
                        }

                        if (Objects.isNull(fieldStringValue))
                            preparedStatement.setNull(p.getParameterOrder(), Types.VARCHAR);
                        else
                            preparedStatement.setString(p.getParameterOrder(), fieldStringValue);
                        break;
                    case BOOLEAN:
                        boolean fieldBooleanValue = false;
                        if (!p.getEtlField()) {
                            switch (sourceFormat) {
                                case "CSV":
                                    fieldBooleanValue = (lambdaCSVRecord != null && lambdaCSVRecord.length > p.getParameterOrder()) ? CommonUtils.getSQLBoolean(lambdaCSVRecord[p.getParameterOrder() - 1]) : false;
                                    break;
                                case "JSON":
                                    fieldBooleanValue = (lambdaJSONRecord != null && lambdaJSONRecord.containsKey(p.getParameterName()) && lambdaJSONRecord.get(p.getParameterName()) != null ? CommonUtils.getSQLBoolean(lambdaJSONRecord.get(p.getParameterName()).toString()) : false);
                                    break;
                            }
                        }
                        preparedStatement.setBoolean(p.getParameterOrder(), fieldBooleanValue);
                        break;

                    case BIGINT:
                        Long fieldLongValue = null;
                        if (!p.getEtlField()) {
                            switch (sourceFormat) {
                                case "CSV":
                                    fieldLongValue = (lambdaCSVRecord != null && lambdaCSVRecord.length > p.getParameterOrder()) ? Long.parseLong(lambdaCSVRecord[p.getParameterOrder() - 1]) : null;
                                    break;
                                case "JSON":
                                    fieldLongValue = (lambdaJSONRecord != null && lambdaJSONRecord.containsKey(p.getParameterName()) && lambdaJSONRecord.get(p.getParameterName()) != null ? (Long) lambdaJSONRecord.get(p.getParameterName()) : 0);
                                    break;
                            }
                        }
                        if (Objects.isNull(fieldLongValue))
                            preparedStatement.setNull(p.getParameterOrder(), Types.BIGINT);
                        else
                            preparedStatement.setLong(p.getParameterOrder(), fieldLongValue);
                        break;
                    case DOUBLE:
                        Double fieldDoubleValue = null;
                        if (!p.getEtlField()) {
                            switch (sourceFormat) {
                                case "CSV":
                                    fieldDoubleValue = (lambdaCSVRecord != null && lambdaCSVRecord.length > p.getParameterOrder()) ? Double.parseDouble(lambdaCSVRecord[p.getParameterOrder() - 1]) : null;
                                    break;
                                case "JSON":
                                    fieldDoubleValue = (lambdaJSONRecord != null && lambdaJSONRecord.containsKey(p.getParameterName()) && lambdaJSONRecord.get(p.getParameterName()) != null ? (Double) lambdaJSONRecord.get(p.getParameterName()) : null);
                                    break;
                            }
                        }
                        if (Objects.isNull(fieldDoubleValue))
                            preparedStatement.setNull(p.getParameterOrder(), Types.DECIMAL);
                        else
                            preparedStatement.setDouble(p.getParameterOrder(), fieldDoubleValue);
                        break;
                    case DATE:
                        Date fieldDateValue = null;
                        if (!p.getEtlField())
                            switch (sourceFormat) {
                                case "CSV":
                                    fieldDateValue = (lambdaCSVRecord != null && lambdaCSVRecord.length > p.getParameterOrder()) ? CommonUtils.getSQLDate(lambdaCSVRecord[p.getParameterOrder() - 1], p.getDateFormat()) : null;
                                    break;
                                case "JSON":
                                    fieldDateValue = (lambdaJSONRecord != null && lambdaJSONRecord.containsKey(p.getParameterName()) && lambdaJSONRecord.get(p.getParameterName()) != null ? CommonUtils.getSQLDate(lambdaJSONRecord.get(p.getParameterName()).toString(), p.getDateFormat()) : null);
                                    break;
                            }
                        if (Objects.isNull(fieldDateValue))
                            preparedStatement.setNull(p.getParameterOrder(), Types.DATE);
                        else
                            preparedStatement.setDate(p.getParameterOrder(), fieldDateValue);
                        break;
                    case INTEGER:
                        Integer fieldIntegerValue = null;
                        if (!p.getEtlField()) {
                            if (p.getParameterName().equals("source_id"))
                                fieldIntegerValue = sourceId;
                            else {
                                switch (sourceFormat) {
                                    case "CSV":
                                        fieldIntegerValue = (lambdaCSVRecord != null && lambdaCSVRecord.length > p.getParameterOrder()) ? Integer.parseInt(lambdaCSVRecord[p.getParameterOrder() - 1]) : null;
                                        break;
                                    case "JSON":
                                        fieldIntegerValue = (lambdaJSONRecord != null && lambdaJSONRecord.containsKey(p.getParameterName()) && lambdaJSONRecord.get(p.getParameterName()) != null ? (Integer) lambdaJSONRecord.get(p.getParameterName()) : null);
                                        break;
                                }
                            }
                        }
                        if (Objects.isNull(fieldIntegerValue))
                            preparedStatement.setNull(p.getParameterOrder(), Types.INTEGER);
                        else
                            preparedStatement.setInt(p.getParameterOrder(), fieldIntegerValue);

                        break;
                    case TIMESTAMP:
                        Timestamp fieldTimeStampValue = null;
                        if (p.getEtlField()) {
                            if (p.getParameterName().equals(GlobalConstant.ETL_COLUMN_NAME.etl_ingest_datetime.toString())) {
                                fieldTimeStampValue = new Timestamp(System.currentTimeMillis());
                            }
                        } else {
                            switch (sourceFormat) {
                                case "CSV":
                                    fieldTimeStampValue = (lambdaCSVRecord != null && lambdaCSVRecord.length > p.getParameterOrder()) ? CommonUtils.getSQLTimestamp(lambdaCSVRecord[p.getParameterOrder() - 1]) : null;
                                    break;
                                case "JSON":
                                    fieldTimeStampValue = (lambdaJSONRecord != null && lambdaJSONRecord.containsKey(p.getParameterName()) && lambdaJSONRecord.get(p.getParameterName()) != null ? CommonUtils.getSQLTimestamp(lambdaJSONRecord.get(p.getParameterName()).toString()) : null);
                                    break;
                            }
                        }
                        if (Objects.isNull(fieldTimeStampValue))
                            preparedStatement.setNull(p.getParameterOrder(), Types.TIMESTAMP);
                        else
                            preparedStatement.setTimestamp(p.getParameterOrder(), fieldTimeStampValue);
                        break;
                }

            } catch (SQLException e) {
                CommonUtils.LogToSystemOut(String.format("Line Number: %d. Column: %s", lineNumber, p));
                e.printStackTrace();
            }
        });

        //      CommonUtils.LogToSystemOut(preparedStatement.toString());

    }


    public static void logSourceTrackerIU(IngestionTrackerDetail ingestionTrackerDetail) {
        try {

            PreparedStatement sourceTrackerIU = dbConnection.prepareStatement("SELECT * FROM monitor.f_ingestion_tracker_iu(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            Integer iParam = 1;
            if (Objects.isNull(ingestionTrackerDetail.getSourceTrackerId()))
                sourceTrackerIU.setNull(iParam, Types.BIGINT);
            else
                sourceTrackerIU.setLong(iParam, ingestionTrackerDetail.getSourceTrackerId());
            iParam++;
            if (Objects.isNull(ingestionTrackerDetail.getSourceKey()))
                sourceTrackerIU.setNull(iParam, Types.VARCHAR);
            else
                sourceTrackerIU.setString(iParam, ingestionTrackerDetail.getSourceKey());
            iParam++;
            if (Objects.isNull(ingestionTrackerDetail.getSourceType()))
                sourceTrackerIU.setNull(iParam, Types.VARCHAR);
            else
                sourceTrackerIU.setString(iParam, ingestionTrackerDetail.getSourceType().toString());
            iParam++;
            if (Objects.isNull(ingestionTrackerDetail.getRawFileName()))
                sourceTrackerIU.setNull(iParam, Types.VARCHAR);
            else
                sourceTrackerIU.setString(iParam, ingestionTrackerDetail.getRawFileName());
            iParam++;
            if (Objects.isNull(ingestionTrackerDetail.getSourceBucket()))
                sourceTrackerIU.setNull(iParam, Types.VARCHAR);
            else
                sourceTrackerIU.setString(iParam, ingestionTrackerDetail.getSourceBucket());
            iParam++;
            if (Objects.isNull(ingestionTrackerDetail.getSourcePrefix()))
                sourceTrackerIU.setNull(iParam, Types.VARCHAR);
            else
                sourceTrackerIU.setString(iParam, ingestionTrackerDetail.getSourcePrefix());
            iParam++;
            if (Objects.isNull(ingestionTrackerDetail.getTargetBucket()))
                sourceTrackerIU.setNull(iParam, Types.VARCHAR);
            else
                sourceTrackerIU.setString(iParam, ingestionTrackerDetail.getTargetBucket());
            iParam++;
            if (Objects.isNull(ingestionTrackerDetail.getTargetPrefix()))
                sourceTrackerIU.setNull(iParam, Types.VARCHAR);
            else
                sourceTrackerIU.setString(iParam, ingestionTrackerDetail.getTargetPrefix());
            iParam++;
            if (Objects.isNull(ingestionTrackerDetail.getStageDBTable()))
                sourceTrackerIU.setNull(iParam, Types.VARCHAR);
            else
                sourceTrackerIU.setString(iParam, ingestionTrackerDetail.getStageDBTable());
            iParam++;
            if (Objects.isNull(ingestionTrackerDetail.getInsertRowCount()))
                sourceTrackerIU.setNull(iParam, Types.INTEGER);
            else
                sourceTrackerIU.setInt(iParam, ingestionTrackerDetail.getInsertRowCount());
            iParam++;
            if (Objects.isNull(ingestionTrackerDetail.getLambdaEventTriggerTime()))
                sourceTrackerIU.setNull(iParam, Types.TIMESTAMP);
            else
                sourceTrackerIU.setTimestamp(iParam, ingestionTrackerDetail.getLambdaEventTriggerTime());
            iParam++;
            if (Objects.isNull(ingestionTrackerDetail.getStageWriteTime()))
                sourceTrackerIU.setNull(iParam, Types.TIMESTAMP);
            else
                sourceTrackerIU.setTimestamp(iParam, ingestionTrackerDetail.getStageWriteTime());
            iParam++;
            if (Objects.isNull(ingestionTrackerDetail.getWarehouseWriteTime()))
                sourceTrackerIU.setNull(iParam, Types.TIMESTAMP);
            else
                sourceTrackerIU.setTimestamp(iParam, ingestionTrackerDetail.getWarehouseWriteTime());
            iParam++;
            if (Objects.isNull(ingestionTrackerDetail.getRdzWriteTime()))
                sourceTrackerIU.setNull(iParam, Types.TIMESTAMP);
            else
                sourceTrackerIU.setTimestamp(iParam, ingestionTrackerDetail.getRdzWriteTime());
            iParam++;
            if (Objects.isNull(ingestionTrackerDetail.getIngestionStatus()))
                sourceTrackerIU.setNull(iParam, Types.INTEGER);
            else
                sourceTrackerIU.setInt(iParam, ingestionTrackerDetail.getIngestionStatus());
            iParam++;
            if (Objects.isNull(ingestionTrackerDetail.getStatusMessage()))
                sourceTrackerIU.setNull(iParam, Types.VARCHAR);
            else
                sourceTrackerIU.setString(iParam, ingestionTrackerDetail.getStatusMessage());

            ResultSet sourceTrackerIdResult = sourceTrackerIU.executeQuery();
            if (!Objects.isNull(sourceTrackerIdResult) && sourceTrackerIdResult.next()) {
                CommonUtils.LogToSystemOut(String.format("Executing %s", sourceTrackerIU));
                ingestionTrackerDetail.setSourceTrackerId(sourceTrackerIdResult.getLong(1));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void logSourceFileLookup(IngestSourceDetail ingestSourceDetail) {
        try {
            String logSourceLookupInsertSQL = "INSERT INTO monitor.source_file_lookup(source_id, raw_file_name, bucket, prefix, ingestion_status_id,status_message) VALUES (?, ?, ?, ?, ?, ?)";

            PreparedStatement psSorceLookupInsert = dbConnection.prepareStatement(logSourceLookupInsertSQL);
            psSorceLookupInsert.setInt(1, ingestSourceDetail.getDbSourceId());
            psSorceLookupInsert.setString(2, ingestSourceDetail.getRawFileName());
            psSorceLookupInsert.setString(3, ingestSourceDetail.getS3HelperRequest().getToBucket());
            psSorceLookupInsert.setString(4, ingestSourceDetail.getS3HelperRequest().getToKey());
            psSorceLookupInsert.setInt(5, ingestSourceDetail.getIngestionTrackerDetail().getIngestionStatus());
            psSorceLookupInsert.setString(6, ingestSourceDetail.getStatusMessage());
            psSorceLookupInsert.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }


}
