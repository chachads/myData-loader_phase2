ALTER TABLE lookup.lookup_property ADD CONSTRAINT lookup_property_source_fkey FOREIGN KEY (source_id) REFERENCES lookup.lookup_source(internal_source_id);
ALTER TABLE lookup.lookup_xref ADD CONSTRAINT lookup_xref_key_fkey FOREIGN KEY (lookup_key_id) REFERENCES lookup.lookup_key(lookup_key_id);
ALTER TABLE lookup.lookup_xref ADD CONSTRAINT lookup_xref_property_fkey FOREIGN KEY (property_id) REFERENCES lookup.lookup_property(internal_property_id);
ALTER TABLE warehouse.reservation_stay_date_f ADD CONSTRAINT reservation_property_fk FOREIGN KEY (internal_property_id) REFERENCES lookup.lookup_property(internal_property_id);
ALTER TABLE warehouse.reservation_stay_date_f ADD CONSTRAINT reservation_reservation_fk FOREIGN KEY (internal_reservation_id) REFERENCES lookup.lookup_reservation(internal_reservation_id);
ALTER TABLE warehouse.reservation_stay_date_f ADD CONSTRAINT reservation_source_fk FOREIGN KEY (source_id) REFERENCES lookup.lookup_source(internal_source_id);
