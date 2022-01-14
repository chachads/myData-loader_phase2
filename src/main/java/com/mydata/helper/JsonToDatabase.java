package com.mydata.helper;

import com.mydata.entity.DBHelperRequest;
import com.mydata.entity.GlobalConstant;
import com.mydata.entity.domain.IngestSourceDetail;

import java.sql.Connection;

public class JsonToDatabase {

    public static Connection ConnectToDB() throws Exception {
        DBHelperRequest dbHelperRequest = new DBHelperRequest("jdbc:postgresql://localhost:5432/mydata", "newuser", "pwd");
        IDBHelper dbHelper = new DBHelper(dbHelperRequest);
        Connection conn = dbHelper.getConnection("mydata", false);
        return conn;
    }

    public static void main(String args[]) {
        IngestSourceDetail isd = new IngestSourceDetail("ldz/ONQ_PMSLEDGER/subset.json", "mydata-poc");
        String filePath = "/Users/chachads/Downloads/LEDGER_Highgate_Hotels_20211214_20211215_1230.json";
        isd.setLocalFilePath(filePath);
        DBHelperRequest dbHelperRequest = new DBHelperRequest("jdbc:postgresql://localhost:5432/mydata", "newuser", "pwd");
        DBHelper dbHelper = new DBHelper(dbHelperRequest);
        dbHelper.getParamFieldListBySource(GlobalConstant.SOURCE_KEY.OPERA,true);
        dbHelper.getParamFieldListBySource(GlobalConstant.SOURCE_KEY.OPERA,false);
        //dbHelper.readAndLoadJSON(isd);
    }
/*
        String[] paramList = new String[45];
        Arrays.fill(paramList,"?");
        String paramString = String.join(",",paramList);
        //Creating a JSONParser object
        JSONParser jsonParser = new JSONParser();
        try {
            //Parsing the contents of the JSON file
            String filePath = "/Users/chachads/Downloads/subset.json";
            InputStream targetStream = FileUtils.openInputStream(new File(filePath));

            JSONArray jarray = (JSONArray) jsonParser.parse(new FileReader(filePath));
//            JSONArray jarray = (JSONArray) jsonParser.parse(new InputStreamReader(targetStream, StandardCharsets.UTF_8));
            Connection con = ConnectToDB();
            //Insert a row into the MyPlayers table
            PreparedStatement pstmt = con.prepareStatement("INSERT INTO MyPlayers values (?, ?, ?, ?, ?, ? )");
            for (Object object : jarray) {
                JSONObject record = (JSONObject) object;
                int id = Integer.parseInt((String) record.get("ID"));
                String first_name = (String) record.get("First_Name");
                String last_name = (String) record.get("Last_Name");
                String date = (String) record.get("Date_Of_Birth");
                long date_of_birth = Date.valueOf(date).getTime();
                String place_of_birth = (String) record.get("Place_Of_Birth");
                String country = (String) record.get("Country");
                pstmt.setInt(1, id);
                pstmt.setString(2, first_name);
                pstmt.setString(3, last_name);
                pstmt.setDate(4, new Date(date_of_birth));
                pstmt.setString(5, place_of_birth);
                pstmt.setString(6, country);
                pstmt.executeUpdate();
            }
            System.out.println("Records inserted.....");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }*/
}

/*
 |-- accounting_category: string (nullable = true)
 |-- accounting_id: string (nullable = true)
 |-- accounting_id_desc: string (nullable = true)
 |-- accounting_type: string (nullable = true)
 |-- ar_account_id: string (nullable = true)
 |-- ar_account_key: string (nullable = true)
 |-- ar_code: string (nullable = true)
 |-- ar_description: string (nullable = true)
 |-- ar_type_code: string (nullable = true)
 |-- ar_type_sub_code: string (nullable = true)
 |-- business_date: string (nullable = true)
 |-- charge_category: string (nullable = true)
 |-- charge_routed: string (nullable = true)
 |-- common_account_identifier: string (nullable = true)
 |-- confirmation_number: string (nullable = true)
 |-- crs_inn_code: string (nullable = true)
 |-- employee_id: string (nullable = true)
 |-- entry_currency_code: string (nullable = true)
 |-- entry_datetime: string (nullable = true)
 |-- entry_id: long (nullable = true)
 |-- entry_type: string (nullable = true)
 |-- exchange_rate: double (nullable = true)
 |-- facility_id: string (nullable = true)
 |-- foreign_amount: double (nullable = true)
 |-- gl_account_id: string (nullable = true)
 |-- gnr: string (nullable = true)
 |-- group_key: string (nullable = true)
 |-- group_name: string (nullable = true)
 |-- hhonors_receipt_ind: long (nullable = true)
 |-- house_key: string (nullable = true)
 |-- include_in_net_use: string (nullable = true)
 |-- inncode: string (nullable = true)
 |-- insert_datetime_utc: string (nullable = true)
 |-- ledger_entry_amount: double (nullable = true)
 |-- original_folio_id: long (nullable = true)
 |-- original_receipt_id: long (nullable = true)
 |-- original_stay_id: long (nullable = true)
 |-- owner_account_id: string (nullable = true)
 |-- owner_account_name: string (nullable = true)
 |-- owner_extract_type: string (nullable = true)
 |-- partition_date: string (nullable = true)
 |-- pms_inn_code: string (nullable = true)
 |-- posting_type_code: string (nullable = true)
 |-- receipt_id: long (nullable = true)
 |-- routed_to_folio: long (nullable = true)
 |-- stay_id: long (nullable = true)
 |-- trans_desc: string (nullable = true)
 |-- trans_id: long (nullable = true)
 |-- trans_travel_reason_code: string (nullable = true)
 |-- version: string (nullable = true)


 */