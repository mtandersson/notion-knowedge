-- Labels are mutable metadata; existing identity and uniqueness remain unchanged.
ALTER TABLE graph_edges ADD COLUMN display_name TEXT;
