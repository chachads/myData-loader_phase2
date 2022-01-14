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


    /**
     * Creates a dbHelper class. A request object required.
     *
     * @param dbHelperRequest - request object for DBHelper. {@link DBHelperRequest}
     */
    public DBHelper(DBHelperRequest dbHelperRequest) {
        this.dbHelperRequest = dbHelperRequest;
    }

    public Connection getConnection(String dbName, Boolean autoCommit) throws SQLException {
        // TODO - add db name to connection string.
        System.out.println("GETTING CONNECTION");
        System.out.println(String.format("URL: %s and UID: %s PWD %s", dbHelperRequest.getDbURL(), dbHelperRequest.getDbUID(), dbHelperRequest.getDbPWD()));
        Connection conn = DriverManager.getConnection(dbHelperRequest.getDbURL(), dbHelperRequest.getDbUID(), dbHelperRequest.getDbPWD());
        conn.setAutoCommit(autoCommit);
        System.out.println("RETURNING CONNECTION");
        return conn;
    }

    @Override
    public void loadStream(IngestSourceDetail ingestSourceDetail, InputStream streamToLoad) {
        final String copyCSVSQL = "COPY %s FROM STDIN (DELIMITER ',',FORMAT csv,HEADER true,NULL 'NULL')";
        final String insertToStageSQL = "INSERT INTO %s (%s) SELECT DISTINCT '%s',%d,%s,'%s',current_timestamp FROM %s";
        final String createTempTableSQL = "CREATE TEMP TABLE %s AS SELECT * FROM %s";
        String tempTableName = "t" + UUID.randomUUID().toString().replace("-", "");
        String etlBatchId = tempTableName;
        try {
            String insertSQLParam1 = ingestSourceDetail.getStageTableName();
            String insertSQLParam2 = getParamFieldListBySource(ingestSourceDetail.getESourceKey(),false);
            String insertSQLParam3 = etlBatchId;
            Integer insertSQLParam4 = ingestSourceDetail.getDbSourceId();
            String insertSQLParam5= getParamFieldListBySource(ingestSourceDetail.getESourceKey(),true);
            String insertSQLParam6 = ingestSourceDetail.getRawFileName();
            String insertSQLParam7 = tempTableName;
            String tempTableDefinition = ingestSourceDetail.getTempTableDefinition();
            Connection conn = getConnection("mydata", false);
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
            System.out.println(String.format("Insert to Stage SQL: %s",insertToStageSQL));

            String insertToStage = String.format(insertToStageSQL, insertSQLParam1,insertSQLParam2,insertSQLParam3,insertSQLParam4,insertSQLParam5,insertSQLParam6,insertSQLParam7);
            System.out.println(insertToStage);
            PreparedStatement copyToStage = conn.prepareStatement(insertToStage);
            copyToStage.execute();
            conn.commit();
            conn.close();
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
            Connection connection = getConnection("mydata", false);
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
                createPreparedStatement(preparedStatement, (JSONObject) object, sourceFieldParameterList, etlBatchId, ingestSourceDetail.getRawFileName());
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

    protected void createPreparedStatement(PreparedStatement preparedStatement, JSONObject record, List<SourceFieldParameter> sourceFieldParameterList, String etlBatchId, String etlFileName) {
        sourceFieldParameterList.forEach(p -> {
            GlobalConstant.PSQL_PARAMETER_TYPE fieldType = p.getParameterType();
            try {
                switch (fieldType) {
                    case CHARACTER_VARYING:
                        if (p.getEtlField())
                            if (p.getParameterName() == GlobalConstant.ETL_COLUMN_NAME.etl_batch_id.toString())
                                preparedStatement.setString(p.getParameterOrder(), etlBatchId);
                            else if (p.getParameterName() == GlobalConstant.ETL_COLUMN_NAME.etl_file_name.toString())
                                preparedStatement.setString(p.getParameterOrder(), etlFileName);
                            else
                                preparedStatement.setString(p.getParameterOrder(), "");
                        else
                            preparedStatement.setString(p.getParameterOrder(), (record.containsKey(p.getParameterName()) && record.get(p.getParameterName()) != null) ? record.get(p.getParameterName()).toString() : null);
                        break;
                    case BOOLEAN:
                        Boolean fieldBooleanValue = false;
                        if (p.getEtlField())
                            fieldBooleanValue = false;
                        else
                            fieldBooleanValue = (record.containsKey(p.getParameterName()) && record.get(p.getParameterName()) != null ? getSQLBoolean(record.get(p.getParameterName()).toString()) : false);
                        preparedStatement.setBoolean(p.getParameterOrder(), fieldBooleanValue);
                        break;

                    case BIGINT:
                        Long fieldLongValue = null;
                        if (p.getEtlField())
                            fieldLongValue = null;
                        else
                            fieldLongValue = (record.containsKey(p.getParameterName()) && record.get(p.getParameterName()) != null ? (Long) record.get(p.getParameterName()) : 0);
                        preparedStatement.setLong(p.getParameterOrder(), fieldLongValue);
                        break;
                    case DOUBLE:
                        Double fieldDoubleValue = null;
                        if (p.getEtlField())
                            fieldDoubleValue = null;
                        else
                            fieldDoubleValue = (record.containsKey(p.getParameterName()) && record.get(p.getParameterName()) != null ? (Double) record.get(p.getParameterName()) : null);
                        preparedStatement.setDouble(p.getParameterOrder(), fieldDoubleValue);
                        break;
                    case DATE:
                        Date fieldDateValue = null;
                        if (p.getEtlField())
                            fieldDateValue = null;
                        else
                            fieldDateValue = (record.containsKey(p.getParameterName()) && record.get(p.getParameterName()) != null ? getSQLDate(record.get(p.getParameterName()).toString()) : null);
                        preparedStatement.setDate(p.getParameterOrder(), fieldDateValue);
                        break;
                    case INTEGER:
                        Integer fieldIntegerValue = null;
                        if (p.getEtlField())
                            fieldIntegerValue = null;
                        else
                            fieldIntegerValue = (record.containsKey(p.getParameterName()) && record.get(p.getParameterName()) != null ? (Integer) record.get(p.getParameterName()) : null);
                        preparedStatement.setInt(p.getParameterOrder(), fieldIntegerValue);
                        break;
                    case TIMESTAMP:
                        Timestamp fieldTimeStampValue = null;
                        if (p.getEtlField())
                            if (p.getParameterName() == GlobalConstant.ETL_COLUMN_NAME.etl_ingest_datetime.toString())
                                fieldTimeStampValue = new Timestamp(System.currentTimeMillis());
                            else
                                fieldTimeStampValue = null;
                        else
                            fieldTimeStampValue = (record.containsKey(p.getParameterName()) && record.get(p.getParameterName()) != null ? getSQLTimestamp(record.get(p.getParameterName()).toString()) : null);
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
            Connection connection = getConnection("mydata", true);
            PreparedStatement fileTrackerInsert = connection.prepareStatement("INSERT INTO monitor.source_tracker(" +
                    "source_type,raw_file_name, source_bucket, source_key, target_bucket, target_key, target_db_table, insert_row_count, process_start_time, process_db_write_time, process_rdz_write_time) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
            int paramId = 1;
            fileTrackerInsert.setString(paramId,sourceTrackerDetail.getSourceType().toString());
            paramId++;
            fileTrackerInsert.setString(paramId, sourceTrackerDetail.getRawFileName());
            paramId++;
            fileTrackerInsert.setString(paramId, sourceTrackerDetail.getSourceBucket());
            paramId++;
            fileTrackerInsert.setString(paramId, sourceTrackerDetail.getSourceKey());
            paramId++;
            fileTrackerInsert.setString(paramId, sourceTrackerDetail.getTargetBucket());
            paramId++;
            fileTrackerInsert.setString(paramId, sourceTrackerDetail.getTargetKey());
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
            System.out.println(fileTrackerInsert.toString());
            fileTrackerInsert.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    protected Boolean getSQLBoolean(String booleanValue) {
        if (booleanValue == null || booleanValue.equals("0") || booleanValue.toUpperCase().equals("FALSE"))
            return false;
        else
            return (booleanValue.equals("1") || booleanValue.toUpperCase().equals("TRUE"));
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
        final String sourceDefintionSelectSQL = "SELECT * from lookup.f_get_lookup_source('%s')";
        System.out.println(String.format(sourceDefintionSelectSQL, ingestSourceDetail.getSourceKey().toString()));
        PreparedStatement sourceSelection = getConnection("mydata", true).prepareStatement(String.format(sourceDefintionSelectSQL, ingestSourceDetail.getSourceKey().toString()));
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

    protected Integer getParameterCount(GlobalConstant.SOURCE_KEY sourceKey) {
        return GlobalConstant.sourceFieldParameterList.stream().filter(p -> p.getSourceKey().equals(sourceKey)).collect(Collectors.toList()).size();
    }

    protected String getParamQueryStringBySource(GlobalConstant.SOURCE_KEY sourceKey) {
        Integer columnCount = GlobalConstant.sourceFieldParameterList.stream().filter(p -> p.getSourceKey().equals(sourceKey)).collect(Collectors.toList()).size();
        String[] paramList = new String[columnCount];
        Arrays.fill(paramList, "?");
        return String.join(",", paramList);
    }

    protected String getParamFieldListBySource(GlobalConstant.SOURCE_KEY sourceKey,boolean excludeETLFields){
    List<SourceFieldParameter> paramList = GlobalConstant.sourceFieldParameterList.stream().filter(p -> p.getSourceKey().equals(sourceKey) && ((p.getEtlField() == false && excludeETLFields) || !excludeETLFields )).collect(Collectors.toList());
    List<String> fieldList = new ArrayList<>();
    paramList.forEach(f-> {
        fieldList.add(f.getParameterName());
    });
    System.out.println(String.format("Exclude ETL: %b. Field List: %s",excludeETLFields,String.join(",",fieldList)));
    // now join the array.
    return String.join(",",fieldList);
        //return paramList.stream().jo;
}

}
