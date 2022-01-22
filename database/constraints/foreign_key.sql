ALTER TABLE lookup.lookup_property ADD CONSTRAINT lookup_property_source_fkey FOREIGN KEY (source_id) REFERENCES lookup.lookup_source(internal_source_id);
ALTER TABLE lookup.lookup_xref ADD CONSTRAINT lookup_xref_key_fkey FOREIGN KEY (lookup_key_id) REFERENCES lookup.lookup_key(lookup_key_id);
ALTER TABLE lookup.lookup_xref ADD CONSTRAINT lookup_xref_property_fkey FOREIGN KEY (property_id) REFERENCES lookup.lookup_property(internal_property_id);
ALTER TABLE warehouse.reservation_stay_date_f ADD CONSTRAINT reservation_sd_property_fk FOREIGN KEY (internal_property_id) REFERENCES lookup.lookup_property(internal_property_id);
ALTER TABLE warehouse.reservation_stay_date_f ADD CONSTRAINT reservation_sd_reservation_fk FOREIGN KEY (internal_reservation_id) REFERENCES lookup.lookup_reservation(internal_reservation_id);
ALTER TABLE warehouse.reservation_stay_date_f ADD CONSTRAINT reservation_sd_source_fk FOREIGN KEY (source_id) REFERENCES lookup.lookup_source(internal_source_id);
ALTER TABLE warehouse.reservation_business_date_f ADD CONSTRAINT reservation_bd_property_fk FOREIGN KEY (internal_property_id) REFERENCES lookup.lookup_property(internal_property_id);
ALTER TABLE warehouse.reservation_business_date_f ADD CONSTRAINT reservation_bd_reservation_fk FOREIGN KEY (internal_reservation_id) REFERENCES lookup.lookup_reservation(internal_reservation_id);
ALTER TABLE warehouse.reservation_business_date_f ADD CONSTRAINT reservation_bd_source_fk FOREIGN KEY (source_id) REFERENCES lookup.lookup_source(internal_source_id);
ALTER TABLE warehouse.reservation_business_date_extension_f ADD CONSTRAINT reservation_ext_property_fk FOREIGN KEY (internal_property_id) REFERENCES lookup.lookup_property(internal_property_id);
ALTER TABLE warehouse.reservation_business_date_extension_f ADD CONSTRAINT reservation_ext_reservation_fk FOREIGN KEY (internal_reservation_id) REFERENCES lookup.lookup_reservation(internal_reservation_id);
ALTER TABLE warehouse.reservation_business_date_extension_f ADD CONSTRAINT reservation_ext_source_fk FOREIGN KEY (source_id) REFERENCES lookup.lookup_source(internal_source_id);
ALTER TABLE monitor.source_file_lookup ADD CONSTRAINT source_file_lookup_source_id_fk FOREIGN KEY (source_id) REFERENCES lookup.lookup_source(internal_source_id);
ALTER TABLE monitor.source_file_lookup ADD CONSTRAINT source_file_lookup_ingestion_status_fk FOREIGN KEY (ingestion_status_id) REFERENCES lookup.lookup_ingestion_status(internal_status_id);
ALTER TABLE monitor.ingestion_tracker ADD CONSTRAINT ingestion_tracker_ingestion_status_fk FOREIGN KEY (ingestion_status_id) REFERENCES lookup.lookup_ingestion_status(internal_status_id);