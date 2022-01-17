package com.mydata.helper;

import com.mydata.entity.domain.IngestSourceDetail;
import com.mydata.entity.tracker.SourceTrackerDetail;

import java.io.InputStream;
import java.sql.SQLException;

public interface IDBHelper {
    void loadStream(IngestSourceDetail ingestSourceDetail, InputStream streamToLoad);

    void refreshSourceDefinition(IngestSourceDetail ingestSourceDetail) throws SQLException;

    //Connection getConnection(String dbName, Boolean autoCommit) throws SQLException;

    void readAndLoadJSON(IngestSourceDetail ingestSourceDetail);

    void saveFileTracker(SourceTrackerDetail sourceTrackerDetail);
}
