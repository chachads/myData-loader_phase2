package com.mydata.entity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public interface GlobalConstant {
   /* public static final List<SourceFieldParameter> sourceFieldParameterList = Collections.unmodifiableList(
            new ArrayList<SourceFieldParameter>() {
                {
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "etl_batch_id", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 1, true));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "accounting_category", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 2));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "accounting_id", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 3));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "accounting_id_desc", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 4));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "accounting_type", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 5));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "ar_account_id", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 6));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "ar_account_key", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 7));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "ar_code", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 8));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "ar_description", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 9));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "ar_type_code", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 10));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "ar_type_sub_code", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 11));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "business_date", PSQL_PARAMETER_TYPE.DATE, null, 12));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "charge_category", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 13));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "charge_routed", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 14));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "common_account_identifier", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 15));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "confirmation_number", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 16));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "crs_inn_code", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 17));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "employee_id", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 18));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "entry_currency_code", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 19));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "entry_datetime", PSQL_PARAMETER_TYPE.TIMESTAMP, null, 20));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "entry_id", PSQL_PARAMETER_TYPE.BIGINT, null, 21));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "entry_type", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 22));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "exchange_rate", PSQL_PARAMETER_TYPE.DOUBLE, "numeric(38,10)", 23));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "facility_id", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 24));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "foreign_amount", PSQL_PARAMETER_TYPE.DOUBLE, "numeric(38,10)", 25));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "gl_account_id", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 26));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "gnr", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 27));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "group_key", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 28));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "group_name", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 29));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "hhonors_receipt_ind", PSQL_PARAMETER_TYPE.BOOLEAN, null, 30));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "house_key", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 31));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "include_in_net_use", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 32));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "inncode", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 33));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "insert_datetime_utc", PSQL_PARAMETER_TYPE.TIMESTAMP, null, 34));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "ledger_entry_amount", PSQL_PARAMETER_TYPE.DOUBLE, "numeric(38,10)", 35));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "original_folio_id", PSQL_PARAMETER_TYPE.BIGINT, "numeric(38,10)", 36));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "original_receipt_id", PSQL_PARAMETER_TYPE.BIGINT, "numeric(38,10)", 37));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "original_stay_id", PSQL_PARAMETER_TYPE.BIGINT, "numeric(38,10)", 38));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "owner_account_id", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 39));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "owner_account_name", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 40));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "owner_extract_type", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 41));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "partition_date", PSQL_PARAMETER_TYPE.DATE, "numeric(38,10)", 42));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "pms_inn_code", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 43));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "posting_type_code", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 44));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "receipt_id", PSQL_PARAMETER_TYPE.BIGINT, "numeric(38,10)", 45));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "routed_to_folio", PSQL_PARAMETER_TYPE.BIGINT, "numeric(38,10)", 46));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "stay_id", PSQL_PARAMETER_TYPE.BIGINT, "numeric(38,10)", 47));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "trans_desc", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 48));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "trans_id", PSQL_PARAMETER_TYPE.BIGINT, "numeric(38,10)", 49));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "trans_travel_reason_code", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 50));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "version", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 51));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "etl_file_name", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 52, true));
                    add(new SourceFieldParameter(SOURCE_KEY.ONQ_PMSLEDGER, "etl_ingest_datetime", PSQL_PARAMETER_TYPE.TIMESTAMP, "numeric(38,10)", 53, true));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "resort", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 0));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "business_date", PSQL_PARAMETER_TYPE.DATE, null, 1));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "reservation_marker", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 2));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "confirmation_no", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 3));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "reservation_id", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 4));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "booking_id", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 5));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "guarantee_code", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 6));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "resv_status", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 7));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "cancellation_date", PSQL_PARAMETER_TYPE.DATE, null, 8));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "stay_date", PSQL_PARAMETER_TYPE.DATE, null, 9));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "begin_date", PSQL_PARAMETER_TYPE.DATE, null, 10));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "actual_check_in_date", PSQL_PARAMETER_TYPE.TIMESTAMP, "numeric(38,10)", 11));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "end_date", PSQL_PARAMETER_TYPE.DATE, null, 12));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "actual_check_out_date", PSQL_PARAMETER_TYPE.TIMESTAMP, "numeric(38,10)", 13));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "stay_rooms", PSQL_PARAMETER_TYPE.INTEGER,null, 14));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "stay_adults", PSQL_PARAMETER_TYPE.INTEGER,null, 15));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "stay_children", PSQL_PARAMETER_TYPE.INTEGER,null, 16));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "rate_code", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 17));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "rate_code_description", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 18));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "room_rate", PSQL_PARAMETER_TYPE.DOUBLE, "numeric(38,10)", 19));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "currency", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 20));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "market_code", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 21));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "source_code", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 22));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "room_class", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 23));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "room_class_description", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 24));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "booked_room_class", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 25));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "booked_room_class_description", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 26));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "room_type", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 27));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "room_type_description", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 28));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "booked_room_type", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 29));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "booked_room_type_description", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 30));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "area_id", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 31));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "room_number", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 32));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "walkin_yn", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 33));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "complimentary_yn", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 34));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "house_use_yn", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 35));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "day_use_yn", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 36));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "discount_amt", PSQL_PARAMETER_TYPE.DOUBLE, "numeric(38,10)", 37));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "discount_prcnt", PSQL_PARAMETER_TYPE.DOUBLE, "numeric(38,10)", 38));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "discount_reason", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 39));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "room_revenue", PSQL_PARAMETER_TYPE.DOUBLE, "numeric(38,10)", 40));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "food_revenue", PSQL_PARAMETER_TYPE.DOUBLE, "numeric(38,10)", 41));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "other_revenue", PSQL_PARAMETER_TYPE.DOUBLE, "numeric(38,10)", 42));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "total_revenue", PSQL_PARAMETER_TYPE.DOUBLE, "numeric(38,10)", 43));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "non_revenue", PSQL_PARAMETER_TYPE.DOUBLE, "numeric(38,10)", 44));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "tax", PSQL_PARAMETER_TYPE.DOUBLE, "numeric(38,10)", 45));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "vip_status", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 46));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "guest_city", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 47));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "guest_country", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 48));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "guest_nationality", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 49));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "membership_id", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 50));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "membership_type", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 51));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "membership_level", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 52));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "membership_class", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 53));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "travel_agent_id", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 54));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "travel_agent_name", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 55));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "travel_agent_address_line_1", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 56));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "travel_agent_city", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 57));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "travel_agent_state", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 58));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "travel_agent_country", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 59));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "travel_agent_postal_code", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 60));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "company_id", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 61));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "company_name", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 62));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "company_address_line_1", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 63));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "company_city", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 64));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "company_state", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 65));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "company_country", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 66));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "company_postal_code", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 67));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "allotment_header_id", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 68));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "resv_name_id", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 69));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "booking_type", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 70));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "block_status", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 71));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "cancellation_code", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 72));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "channel", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 73));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "channel_type", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 74));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "insert_date", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 75));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "update_date", PSQL_PARAMETER_TYPE.TIMESTAMP, "numeric(38,10)", 76));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "etl_batch_id", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 77, true));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "source_id", PSQL_PARAMETER_TYPE.INTEGER,null, 78,true));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "etl_file_name", PSQL_PARAMETER_TYPE.CHARACTER_VARYING, null, 79, true));
                    add(new SourceFieldParameter(SOURCE_KEY.OPERA, "etl_ingest_datetime", PSQL_PARAMETER_TYPE.TIMESTAMP, "numeric(38,10)",80, true));                }
            });
*/
    enum DB_CONNECTION_KEY {
        DB_URL,
        DB_UID,
        DB_PWD,
        DB_SECRETS_NAME,
        DB_PROXY_HOST,
        DB_CONNECTION_STRING
    }

    public enum SOURCE_KEY {
        OPERA,
        ONQ_CRSSTAY,
        ONQ_PMSLEDGER
    }

    public enum PSQL_PARAMETER_TYPE {
        CHARACTER_VARYING,
        INTEGER,
        DOUBLE,
        BIGINT,
        DATE,
        BOOLEAN,
        TIMESTAMP
    }

    public enum ETL_COLUMN_NAME {
        etl_batch_id,
        etl_file_name,
        etl_ingest_datetime
    }

    public enum SOURCE_TYPE {
        FILE
    }
}
