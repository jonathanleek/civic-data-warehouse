CREATE SCHEMA IF NOT EXISTS crosswalk ;

CREATE TABLE IF NOT EXISTS "cdw"."crosswalk"."parcel_xref" (
    region_code varchar(10) NOT NULL, 
    native_ref varchar(64) NOT NULL, 
    parcel_seq text NOT NULL,
    CONSTRAINT parcel_seq_constraint UNIQUE (parcel_seq),
    CONSTRAINT region_and_local_key_constraint UNIQUE (region_code, native_ref)
)
;