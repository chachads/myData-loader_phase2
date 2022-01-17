package com.mydata.helper;

import com.mydata.entity.DBHelperRequest;
import com.mydata.entity.GlobalConstant;
import com.mydata.entity.SourceFieldParameter;
import com.mydata.entity.domain.IngestSourceDetail;
import com.mydata.entity.tracker.SourceTrackerDetail;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Date;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class DBHelper implements IDBHelper {
    private final DBHelperRequest dbHelperRequest;
    private final Connection lamdbaDBConnection;


    /**
     * Creates a dbHelper class. A request object required.
     *
     * @param dbHelperRequest - request object for DBHelper. {@link DBHelperRequest}
     */
    public DBHelper(DBHelperRequest dbHelperRequest, Connection lamdbaDBConnection) {
        this.dbHelperRequest = dbHelperRequest;
        this.lamdbaDBConnection = lamdbaDBConnection;
    }

    /*    public Connection getConnection(String dbName, Boolean autoCommit) {
            // TODO - add db name to connection string.
            System.out.println("GETTING CONNECTION");
            try {
                if (!dbHelperRequest.getDbSecretsRefreshed())
                    DBConnection.refreshDBHelperRequest(dbHelperRequest);
                System.out.println(String.format("URL: %s and UID: %s PWD %s", dbHelperRequest.getDbURL(), dbHelperRequest.getDbUID(), dbHelperRequest.getDbPWD()));
                Connection conn = DriverManager.getConnection(dbHelperRequest.getDbURL(), dbHelperRequest.getDbUID(), dbHelperRequest.getDbPWD());
                conn.setAutoCommit(autoCommit);
                System.out.println("RETURNING CONNECTION");
                return conn;
            }
            catch (SQLException e){
                e.printStackTrace();
            }
            return null;
        }
    */
    @Override
    public void loadStream(IngestSourceDetail ingestSourceDetail, InputStream streamToLoad) {
        final String copyCSVSQL = "COPY %s FROM STDIN (DELIMITER ',',FORMAT csv,HEADER true,NULL 'NULL')";
        final String insertToStageSQL = "INSERT INTO %s (%s) SELECT DISTINCT '%s',%d,%s,'%s',current_timestamp FROM %s";
        final String createTempTableSQL = "CREATE TEMP TABLE %s AS SELECT * FROM %s";
        final String processStageData = "select * from lookup.f_process_stage_opera('%s');";
        String etlBatchId;
        String tempTableName = etlBatchId = "t" + UUID.randomUUID().toString().replace("-", "");
        try {
            String insertSQLParam1 = ingestSourceDetail.getStageTableName();
            String insertSQLParam2 = getParamFieldListBySource(ingestSourceDetail.getESourceKey(), false);
            String insertSQLParam3 = etlBatchId;
            Integer insertSQLParam4 = ingestSourceDetail.getDbSourceId();
            String insertSQLParam5 = getParamFieldListBySource(ingestSourceDetail.getESourceKey(), true);
            String insertSQLParam6 = ingestSourceDetail.getRawFileName();
            String insertSQLParam7 = tempTableName;
            String tempTableDefinition = ingestSourceDetail.getTempTableDefinition();
            Connection conn = ingestSourceDetail.getLambdaDBConnection();
            String sql = String.format(createTempTableSQL, tempTableName, tempTableDefinition);
            System.out.println(String.format("Temp Table SQL is %s", sql));
            PreparedStatement statement = conn.prepareStatement(sql);
            statement.execute();
            System.out.println("temp table createdn now inserting");
            CopyManager cm = new CopyManager((BaseConnection) conn);
            String copyCommand = String.format(copyCSVSQL, tempTableName);
            Long rowsInserted = cm.copyIn(copyCommand, streamToLoad);
            System.out.println(String.format("Rows inserted in temp table %d", rowsInserted));
            // now copy into main staging table with a guid.
            //String copyToStageSQL = String.format(insertToStageSQL, stagingTableName, etlBatchId, ingestSourceDetail.getDbSourceId(), rawFileName, tempTableName);
            System.out.println(String.format("Insert to Stage SQL: %s", insertToStageSQL));

            String insertToStage = String.format(insertToStageSQL, insertSQLParam1, insertSQLParam2, insertSQLParam3, insertSQLParam4, insertSQLParam5, insertSQLParam6, insertSQLParam7);
            System.out.println(insertToStage);
            PreparedStatement copyToStage = conn.prepareStatement(insertToStage);
            copyToStage.execute();
            PreparedStatement processStageStatement = conn.prepareStatement(String.format(processStageData, etlBatchId));
            processStageStatement.execute();
            ingestSourceDetail.getFileTrackerDetail().setInsertRowCount(rowsInserted);
        } catch (SQLException | IOException ex) {
            ex.printStackTrace();
            System.out.println("load stream error");
            System.out.println(ex);
        }

    }

    public void readAndLoadJSON(IngestSourceDetail ingestSourceDetail) {
        try {
            String etlBatchId = "t" + UUID.randomUUID().toString().replace("-", "");
            System.out.println(String.format("inside read and load. Local file name: %s", ingestSourceDetail));
            JSONParser jsonParser = new JSONParser();
            JSONArray jarray = (JSONArray) jsonParser.parse(new FileReader(ingestSourceDetail.getLocalFilePath()));
            System.out.println("finished parsing");
            String paramQueryString = getParamQueryStringBySource(ingestSourceDetail.getESourceKey());
            Connection connection = ingestSourceDetail.getLambdaDBConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(String.format("INSERT INTO %s values (%s)", ingestSourceDetail.getStageTableName(), paramQueryString));
            Long rowCount = 0L;
            Integer batchSize = 1000;
            List<SourceFieldParameter> sourceFieldParameterList = GlobalConstant.sourceFieldParameterList.stream().
                    filter(p -> p.getSourceKey().equals(ingestSourceDetail.getESourceKey())).
                    sorted(Comparator.comparing(SourceFieldParameter::getParameterOrder)).
                    collect(Collectors.toList());
            System.out.println(String.format("Param count is %d", sourceFieldParameterList.size()));
            for (Object object : jarray) {
                rowCount++;
                createPreparedStatement(preparedStatement, ingestSourceDetail.getSourceFormat(), object, sourceFieldParameterList, etlBatchId, ingestSourceDetail.getRawFileName());
                preparedStatement.addBatch();
                if (rowCount % batchSize == 0 || rowCount == jarray.size()) {
                    System.out.println(String.format("COMMITTING BATCH. Row Count: %d", rowCount));
                    int[] rowsInserted = preparedStatement.executeBatch();
                    System.out.println(String.format("COMMITTING BATCH. Row Count: %d. Execute Status: %d", rowCount, rowsInserted.length));
                    connection.commit();
                }
                ingestSourceDetail.getFileTrackerDetail().setInsertRowCount(rowCount);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }



    protected void createPreparedStatement(PreparedStatement preparedStatement, String sourceFormat, Object inputRecord, List<SourceFieldParameter> sourceFieldParameterList, String etlBatchId, String etlFileName) {
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
                                    preparedStatement.setString(p.getParameterOrder(), (lambdaCSVRecord != null && lambdaCSVRecord.length > Integer.valueOf(p.getParameterOrder()) ? lambdaCSVRecord[p.getParameterOrder()] : null));
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
                                    fieldBooleanValue = (lambdaCSVRecord != null && lambdaCSVRecord.length > Integer.valueOf(p.getParameterOrder())) ? getSQLBoolean(lambdaCSVRecord[p.getParameterOrder()]) : null;
                                    break;
                                case "JSON":
                                    fieldBooleanValue = (lambdaJSONRecord != null && lambdaJSONRecord.containsKey(p.getParameterName()) && lambdaJSONRecord.get(p.getParameterName()) != null ? getSQLBoolean(lambdaJSONRecord.get(p.getParameterName()).toString()) : false);
                                    break;
                            }
                        }
                        preparedStatement.setBoolean(p.getParameterOrder(), fieldBooleanValue);
                        break;

                    case BIGINT:
                        Long fieldLongValue = null;
                        if (p.getEtlField())
                            fieldLongValue = null;
                        else {
                            switch (sourceFormat) {
                                case "CSV":
                                    fieldLongValue = (lambdaCSVRecord != null && lambdaCSVRecord.length > Integer.valueOf(p.getParameterOrder())) ? Long.parseLong(lambdaCSVRecord[p.getParameterOrder()]) : null;
                                    break;
                                case "JSON":
                                    fieldLongValue = (lambdaJSONRecord != null && lambdaJSONRecord.containsKey(p.getParameterName()) && lambdaJSONRecord.get(p.getParameterName()) != null ? (Long) lambdaJSONRecord.get(p.getParameterName()) : 0);
                                    break;
                            }
                        }
                        preparedStatement.setLong(p.getParameterOrder(), fieldLongValue);
                        break;
                    case DOUBLE:
                        Double fieldDoubleValue = null;
                        if (p.getEtlField())
                            fieldDoubleValue = null;
                        else {
                            switch (sourceFormat) {
                                case "CSV":
                                    fieldDoubleValue = (lambdaCSVRecord != null && lambdaCSVRecord.length > Integer.valueOf(p.getParameterOrder())) ? Double.parseDouble(lambdaCSVRecord[p.getParameterOrder()]) : null;
                                    break;
                                case "JSON":
                                    fieldDoubleValue = (lambdaJSONRecord != null && lambdaJSONRecord.containsKey(p.getParameterName()) && lambdaJSONRecord.get(p.getParameterName()) != null ? (Double) lambdaJSONRecord.get(p.getParameterName()) : null);
                                    break;
                            }
                        }
                        preparedStatement.setDouble(p.getParameterOrder(), fieldDoubleValue);
                        break;
                    case DATE:
                        Date fieldDateValue = null;
                        if (!p.getEtlField())
                            switch (sourceFormat) {
                                case "CSV":
                                    fieldDateValue = (lambdaCSVRecord != null && lambdaCSVRecord.length > Integer.valueOf(p.getParameterOrder())) ? getSQLDate(lambdaCSVRecord[p.getParameterOrder()]) : null;
                                    break;
                                case "JSON":
                                    fieldDateValue = (lambdaJSONRecord != null && lambdaJSONRecord.containsKey(p.getParameterName()) && lambdaJSONRecord.get(p.getParameterName()) != null ? getSQLDate(lambdaJSONRecord.get(p.getParameterName()).toString()) : null);
                                    break;
                            }
                        preparedStatement.setDate(p.getParameterOrder(), fieldDateValue);
                        break;
                    case INTEGER:
                        Integer fieldIntegerValue = null;
                        if (p.getEtlField())
                            fieldIntegerValue = null;
                        else {
                            switch (sourceFormat) {
                                case "CSV":
                                    fieldIntegerValue = (lambdaCSVRecord != null && lambdaCSVRecord.length > Integer.valueOf(p.getParameterOrder())) ? Integer.parseInt(lambdaCSVRecord[p.getParameterOrder()]) : null;
                                    break;
                                case "JSON":
                                    fieldIntegerValue = (lambdaJSONRecord != null && lambdaJSONRecord.containsKey(p.getParameterName()) && lambdaJSONRecord.get(p.getParameterName()) != null ? (Integer) lambdaJSONRecord.get(p.getParameterName()) : null);
                                    break;
                            }
                        }
                        preparedStatement.setInt(p.getParameterOrder(), fieldIntegerValue);
                        break;
                    case TIMESTAMP:
                        Timestamp fieldTimeStampValue = null;
                        if (p.getEtlField())
                            if (p.getParameterName().equals(GlobalConstant.ETL_COLUMN_NAME.etl_ingest_datetime.toString()))
                                fieldTimeStampValue = new Timestamp(System.currentTimeMillis());
                            else {
                                switch (sourceFormat) {
                                    case "CSV":
                                        fieldTimeStampValue = (lambdaCSVRecord != null && lambdaCSVRecord.length > Integer.valueOf(p.getParameterOrder())) ? getSQLTimestamp(lambdaCSVRecord[p.getParameterOrder()]) : null;
                                        break;
                                    case "JSON":
                                        fieldTimeStampValue = (lambdaJSONRecord != null && lambdaJSONRecord.containsKey(p.getParameterName()) && lambdaJSONRecord.get(p.getParameterName()) != null ? getSQLTimestamp(lambdaJSONRecord.get(p.getParameterName()).toString()) : null);
                                        break;
                                }
                            }
                        preparedStatement.setTimestamp(p.getParameterOrder(), fieldTimeStampValue);
                        break;
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }

        });

        //      System.out.println(preparedStatement.toString());

    }

    public void saveFileTracker(SourceTrackerDetail sourceTrackerDetail) {
        try {
            System.out.println("saving file tracker");
            PreparedStatement fileTrackerInsert = lamdbaDBConnection.prepareStatement("INSERT INTO monitor.source_tracker(" +
                    "source_key,source_type,raw_file_name, source_bucket, source_prefix, target_bucket, target_prefix, target_db_table, insert_row_count, process_start_time, process_db_write_time, process_rdz_write_time) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
            int paramId = 1;
            fileTrackerInsert.setString(paramId, sourceTrackerDetail.getSourceKey());
            paramId++;
            fileTrackerInsert.setString(paramId, sourceTrackerDetail.getSourceType().toString());
            paramId++;
            fileTrackerInsert.setString(paramId, sourceTrackerDetail.getRawFileName());
            paramId++;
            fileTrackerInsert.setString(paramId, sourceTrackerDetail.getSourceBucket());
            paramId++;
            fileTrackerInsert.setString(paramId, sourceTrackerDetail.getSourcePrefix());
            paramId++;
            fileTrackerInsert.setString(paramId, sourceTrackerDetail.getTargetBucket());
            paramId++;
            fileTrackerInsert.setString(paramId, sourceTrackerDetail.getTargetPrefix());
            paramId++;
            fileTrackerInsert.setString(paramId, sourceTrackerDetail.getTargetDBTable());
            paramId++;
            fileTrackerInsert.setLong(paramId, sourceTrackerDetail.getInsertRowCount());
            paramId++;
            fileTrackerInsert.setTimestamp(paramId, sourceTrackerDetail.getProcessStartTime());
            paramId++;
            fileTrackerInsert.setTimestamp(paramId, sourceTrackerDetail.getProcessDBWriteTime());
            paramId++;
            fileTrackerInsert.setTimestamp(paramId, sourceTrackerDetail.getProcessRDZWriteTime());
            System.out.println(fileTrackerInsert);
            fileTrackerInsert.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    protected Boolean getSQLBoolean(String booleanValue) {
        if (booleanValue == null || booleanValue.equals("0") || booleanValue.equalsIgnoreCase("FALSE") || booleanValue.equalsIgnoreCase("N"))
            return false;
        else
            return (booleanValue.equals("1") || booleanValue.equalsIgnoreCase("TRUE") || booleanValue.equalsIgnoreCase("N"));
    }

    protected Timestamp getSQLTimestamp(String timestampValue) {
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


    protected Date getSQLDate(String dateValue) {
        try {
            return java.sql.Date.valueOf(dateValue);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public void refreshSourceDefinition(IngestSourceDetail ingestSourceDetail) throws SQLException {
        System.out.printf("INSIDE GET SOURCE DETAIL");
        final String sourceDefintionSelectSQL = "SELECT * from lookup.f_lookup_source('%s')";
        System.out.println(String.format(sourceDefintionSelectSQL, ingestSourceDetail.getSourceKey().toString()));
        PreparedStatement sourceSelection = lamdbaDBConnection.prepareStatement(String.format(sourceDefintionSelectSQL, ingestSourceDetail.getSourceKey().toString()));
        ResultSet rs = sourceSelection.executeQuery();
        // this should return 1 row.
        if (rs.next()) {
            ingestSourceDetail.setTempTableDefinition(rs.getString("temp_table_definition"));
            ingestSourceDetail.setStageTableName(rs.getString("stage_" +
                    "table_name"));
            ingestSourceDetail.setSourceFormat(rs.getString("source_format"));
            ingestSourceDetail.setDbSourceId(rs.getInt("internal_source_id"));
        }
        System.out.printf(ingestSourceDetail.toString());
    }


    protected String getParamQueryStringBySource(GlobalConstant.SOURCE_KEY sourceKey) {
        Integer columnCount = GlobalConstant.sourceFieldParameterList.stream().filter(p -> p.getSourceKey().equals(sourceKey)).collect(Collectors.toList()).size();
        String[] paramList = new String[columnCount];
        Arrays.fill(paramList, "?");
        return String.join(",", paramList);
    }

    protected String getParamFieldListBySource(GlobalConstant.SOURCE_KEY sourceKey, boolean excludeETLFields) {
        List<SourceFieldParameter> paramList = GlobalConstant.sourceFieldParameterList.stream().filter(p -> p.getSourceKey().equals(sourceKey) && (!p.getEtlField() || !excludeETLFields)).collect(Collectors.toList());
        List<String> fieldList = new ArrayList<>();
        paramList.forEach(f -> fieldList.add(f.getParameterName()));
        System.out.println(String.format("Exclude ETL: %b. Field List: %s", excludeETLFields, String.join(",", fieldList)));
        // now join the array.
        return String.join(",", fieldList);
        //return paramList.stream().jo;
    }

}
