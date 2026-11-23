-- Remove schema 23 additions before constructing an older test database.
DROP TRIGGER source_signal_webhook;
DROP TRIGGER source_signal_mutation;
DROP TABLE source_document_publications;
DROP TABLE source_inventory_watermarks;
DROP TABLE source_change_signals;
DROP TABLE source_signal_clock;
