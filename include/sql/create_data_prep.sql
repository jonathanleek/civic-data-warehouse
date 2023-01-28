create schema data_prep
    create table if not exists dead_records ()
    create table if not exists updated_records ()
    create table if not exists timestamp_metadata (db_event, event_timestamp);