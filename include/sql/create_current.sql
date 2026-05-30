-- =============================================================================
-- CDW "current" schema DDL
-- -----------------------------------------------------------------------------
-- Implements the CDW_ID standard (documentation/schema/cdw_id.md):
--
--     CCCC.cccccc.pppppppp.bbbb.uuuuu
--     country(4) . region/county(6) . parcel(8) . building(4) . unit(5)
--
-- Spine tables (parcel, building, unit) store the CDW_ID as component columns
-- plus a generated STORED text column `cdw_id` that is the zero-padded,
-- dot-delimited concatenation. `cdw_id` is the TEXT PRIMARY KEY. Lower levels
-- not applicable to a given spine row are zero-filled:
--     parcel    -> ....pppppppp.0000.00000
--     building  -> ....pppppppp.bbbb.00000
--     unit      -> ....pppppppp.bbbb.uuuuu
--
-- Non-spine tables use namespaced TEXT surrogate primary keys of the form
--     <country>.<region>.<entity>.<seq>
-- All foreign keys are TEXT to match the keys they reference.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS current ;

-- =============================================================================
-- Lookup tables (namespaced TEXT surrogate keys)
-- These are created first so spine/operations FKs can reference them.
-- =============================================================================

CREATE TABLE IF NOT EXISTS "cdw"."current"."municipality" (
  "municipality_id" text PRIMARY KEY,
  "municipality" varchar
);

CREATE TABLE IF NOT EXISTS "cdw"."current"."division" (
  "division_id" text PRIMARY KEY,
  "division" varchar
);

CREATE TABLE IF NOT EXISTS "cdw"."current"."service_type" (
  "service_type_id" text PRIMARY KEY,
  "service_type" varchar
);

CREATE TABLE IF NOT EXISTS "cdw"."current"."zoning_class" (
  "zoning_class_id" text PRIMARY KEY,
  "zoning_class" varchar
);

CREATE TABLE IF NOT EXISTS "cdw"."current"."use_type" (
  "use_type_id" text PRIMARY KEY,
  "use_type" text
);

CREATE TABLE IF NOT EXISTS "cdw"."current"."permit_type" (
  "permit_type_id" text PRIMARY KEY,
  "permit_type" varchar
);

CREATE TABLE IF NOT EXISTS "cdw"."current"."inspection_type" (
  "inspection_type_id" text PRIMARY KEY,
  "inspection_type" varchar
);

-- =============================================================================
-- Shared entities (namespaced TEXT surrogate keys)
-- =============================================================================

CREATE TABLE IF NOT EXISTS "cdw"."current"."address" (
  "address_id" text PRIMARY KEY,
  "raw_line" text,
  "street_number" int,
  "street_name_prefix" varchar,
  "street_name" varchar,
  "street_name_suffix" varchar,
  "secondary_designator" varchar,
  "city" varchar,
  "state" varchar,
  "zip" text
);

CREATE TABLE IF NOT EXISTS "cdw"."current"."legal_entity" (
  "legal_entity_id" text PRIMARY KEY,
  "name" varchar,
  "address_id" text
);

-- =============================================================================
-- Spine tables (CDW_ID component columns + generated STORED cdw_id PK)
-- =============================================================================

CREATE TABLE IF NOT EXISTS "cdw"."current"."parcel" (
  "country_code" text NOT NULL,
  "region_code" text NOT NULL,
  "parcel_seq" bigint NOT NULL,
  "cdw_id" text GENERATED ALWAYS AS (
    lpad("country_code", 3, '0') || '.' ||
    lpad("region_code", 5, '0') || '.' ||
    lpad("parcel_seq"::text, 8, '0') || '.' ||
    '0000' || '.' ||
    '00000'
  ) STORED PRIMARY KEY,
  "county" varchar,
  "owner_id" text,
  "ward" int,
  "neighborhood" varchar,
  "zip_code" text,
  "census_block" text,
  "frontage" int,
  "zoning_class_id" text
);

CREATE TABLE IF NOT EXISTS "cdw"."current"."building" (
  "country_code" text NOT NULL,
  "region_code" text NOT NULL,
  "parcel_seq" bigint NOT NULL,
  "building_seq" bigint NOT NULL,
  "cdw_id" text GENERATED ALWAYS AS (
    lpad("country_code", 3, '0') || '.' ||
    lpad("region_code", 5, '0') || '.' ||
    lpad("parcel_seq"::text, 8, '0') || '.' ||
    lpad("building_seq"::text, 4, '0') || '.' ||
    '00000'
  ) STORED PRIMARY KEY,
  "parcel_cdw_id" text NOT NULL,
  "owner_id" text,
  "use_type_id" text,
  "sq_footage" int,
  "year_built" int
);

CREATE TABLE IF NOT EXISTS "cdw"."current"."unit" (
  "country_code" text NOT NULL,
  "region_code" text NOT NULL,
  "parcel_seq" bigint NOT NULL,
  "building_seq" bigint NOT NULL,
  "unit_seq" bigint NOT NULL,
  "cdw_id" text GENERATED ALWAYS AS (
    lpad("country_code", 3, '0') || '.' ||
    lpad("region_code", 5, '0') || '.' ||
    lpad("parcel_seq"::text, 8, '0') || '.' ||
    lpad("building_seq"::text, 4, '0') || '.' ||
    lpad("unit_seq"::text, 5, '0')
  ) STORED PRIMARY KEY,
  "building_cdw_id" text NOT NULL,
  "owner_id" text,
  "use_type_id" text,
  "address_id" text,
  "ground_floor" bool,
  "stories" float,
  "windows_ac" int,
  "central_ac" int,
  "full_bath" int,
  "half_bath" int,
  "garage" int
);

-- =============================================================================
-- Operations / event tables (namespaced TEXT surrogate keys)
-- =============================================================================

CREATE TABLE IF NOT EXISTS "cdw"."current"."permit" (
  "permit_id" text PRIMARY KEY,
  "permit_type_id" text,
  "application_date" date,
  "issue_date" date,
  "completion_date" date,
  "cancel_date" date,
  "description" varchar,
  "cost" float,
  "applicant_id" text,
  "contractor_id" text
);

CREATE TABLE IF NOT EXISTS "cdw"."current"."service" (
  "service_id" text PRIMARY KEY,
  "parcel_id" text,
  "building_id" text,
  "unit_id" text,
  "owner_id" text,
  "division_id" text,
  "service_date" date,
  "service_type_id" text,
  "fee" float8
);

CREATE TABLE IF NOT EXISTS "cdw"."current"."condemnation" (
  "condemnation_id" text PRIMARY KEY,
  "inspection_id" text,
  "letter_date" date,
  "condemnation_type" varchar,
  "status" varchar
);

CREATE TABLE IF NOT EXISTS "cdw"."current"."inspection" (
  "inspection_id" text PRIMARY KEY,
  "building_id" text,
  "inspection_date" date,
  "completion_date" date,
  "inspection_type_id" text,
  "number_of_violations" int
);

CREATE TABLE IF NOT EXISTS "cdw"."current"."assessment" (
  "assessment_id" text PRIMARY KEY,
  "parcel_id" text,
  "assessment_date" date,
  "assessment_amount" float,
  "land_value" float,
  "improvement_value" float
);

CREATE TABLE IF NOT EXISTS "cdw"."current"."tax_delinquency" (
  "tax_delinquency_id" text PRIMARY KEY,
  "legal_entity_id" text,
  "last_paid_year" int,
  "amount_delinquent" float
);

-- =============================================================================
-- Native-reference preservation
-- Preserves the city's native parcel reference (the assessor `handle`) and
-- maps it to the CDW parcel cdw_id.
-- NOTE: column previously misspelled `municpal_parcel_id`; corrected here to
--       `municipal_parcel_id` (see report). Low-risk: no transform/DAG code
--       references this column yet.
-- =============================================================================

CREATE TABLE IF NOT EXISTS "cdw"."current"."municipal_parcel_id_mapping" (
  "parcel_id" text PRIMARY KEY,
  "municipal_parcel_id" varchar,
  "municipality_id" text
);

-- =============================================================================
-- Capture tables
-- =============================================================================

-- Exclusion log: records filtered out of the warehouse (e.g. non-real-estate
-- taxable accounts mixed in with parcels). Captures the source natural key, the
-- reason for exclusion, and the raw source payload for later auditing/recovery.
CREATE TABLE IF NOT EXISTS "cdw"."current"."exclusion_log" (
  "exclusion_id" text PRIMARY KEY,
  "source_table" varchar,
  "source_natural_key" varchar,
  "reason" varchar,
  "raw_payload" jsonb,
  "excluded_at" timestamptz DEFAULT now()
);

-- Secondary owner: residual additional owners on multi-owner accounts that
-- cannot be represented by the single parcel.owner_id. One row per extra owner.
CREATE TABLE IF NOT EXISTS "cdw"."current"."secondary_owner" (
  "secondary_owner_id" text PRIMARY KEY,
  "parcel_id" text NOT NULL,
  "owner_id" text NOT NULL,
  "ownership_role" varchar,
  "ownership_share" numeric
);

-- =============================================================================
-- Comments
-- =============================================================================

COMMENT ON COLUMN "cdw"."current"."parcel"."cdw_id" IS 'CDW_ID standard key (CCCC.cccccc.pppppppp.0000.00000); building/unit segments zero-filled at parcel level';
COMMENT ON COLUMN "cdw"."current"."building"."cdw_id" IS 'CDW_ID standard key (CCCC.cccccc.pppppppp.bbbb.00000); unit segment zero-filled at building level';
COMMENT ON COLUMN "cdw"."current"."unit"."cdw_id" IS 'CDW_ID standard key (CCCC.cccccc.pppppppp.bbbb.uuuuu)';
COMMENT ON COLUMN "cdw"."current"."municipal_parcel_id_mapping"."municipal_parcel_id" IS 'City native parcel reference (assessor handle); preserved for crosswalk back to source';

-- =============================================================================
-- Foreign keys
-- =============================================================================

-- Spine parentage (explicit parent FK columns)
ALTER TABLE "cdw"."current"."building" ADD FOREIGN KEY ("parcel_cdw_id") REFERENCES "cdw"."current"."parcel" ("cdw_id");

ALTER TABLE "cdw"."current"."unit" ADD FOREIGN KEY ("building_cdw_id") REFERENCES "cdw"."current"."building" ("cdw_id");

-- Spine -> shared entities / lookups
ALTER TABLE "cdw"."current"."parcel" ADD FOREIGN KEY ("owner_id") REFERENCES "cdw"."current"."legal_entity" ("legal_entity_id");

ALTER TABLE "cdw"."current"."parcel" ADD FOREIGN KEY ("zoning_class_id") REFERENCES "cdw"."current"."zoning_class" ("zoning_class_id");

ALTER TABLE "cdw"."current"."building" ADD FOREIGN KEY ("owner_id") REFERENCES "cdw"."current"."legal_entity" ("legal_entity_id");

ALTER TABLE "cdw"."current"."building" ADD FOREIGN KEY ("use_type_id") REFERENCES "cdw"."current"."use_type" ("use_type_id");

ALTER TABLE "cdw"."current"."unit" ADD FOREIGN KEY ("owner_id") REFERENCES "cdw"."current"."legal_entity" ("legal_entity_id");

ALTER TABLE "cdw"."current"."unit" ADD FOREIGN KEY ("use_type_id") REFERENCES "cdw"."current"."use_type" ("use_type_id");

ALTER TABLE "cdw"."current"."unit" ADD FOREIGN KEY ("address_id") REFERENCES "cdw"."current"."address" ("address_id");

-- Shared entities
ALTER TABLE "cdw"."current"."legal_entity" ADD FOREIGN KEY ("address_id") REFERENCES "cdw"."current"."address" ("address_id");

-- Operations / event tables
ALTER TABLE "cdw"."current"."permit" ADD FOREIGN KEY ("permit_type_id") REFERENCES "cdw"."current"."permit_type" ("permit_type_id");

ALTER TABLE "cdw"."current"."permit" ADD FOREIGN KEY ("applicant_id") REFERENCES "cdw"."current"."legal_entity" ("legal_entity_id");

ALTER TABLE "cdw"."current"."permit" ADD FOREIGN KEY ("contractor_id") REFERENCES "cdw"."current"."legal_entity" ("legal_entity_id");

ALTER TABLE "cdw"."current"."service" ADD FOREIGN KEY ("parcel_id") REFERENCES "cdw"."current"."parcel" ("cdw_id");

ALTER TABLE "cdw"."current"."service" ADD FOREIGN KEY ("building_id") REFERENCES "cdw"."current"."building" ("cdw_id");

ALTER TABLE "cdw"."current"."service" ADD FOREIGN KEY ("unit_id") REFERENCES "cdw"."current"."unit" ("cdw_id");

ALTER TABLE "cdw"."current"."service" ADD FOREIGN KEY ("owner_id") REFERENCES "cdw"."current"."legal_entity" ("legal_entity_id");

ALTER TABLE "cdw"."current"."service" ADD FOREIGN KEY ("division_id") REFERENCES "cdw"."current"."division" ("division_id");

ALTER TABLE "cdw"."current"."service" ADD FOREIGN KEY ("service_type_id") REFERENCES "cdw"."current"."service_type" ("service_type_id");

ALTER TABLE "cdw"."current"."condemnation" ADD FOREIGN KEY ("inspection_id") REFERENCES "cdw"."current"."inspection" ("inspection_id");

ALTER TABLE "cdw"."current"."inspection" ADD FOREIGN KEY ("building_id") REFERENCES "cdw"."current"."building" ("cdw_id");

ALTER TABLE "cdw"."current"."inspection" ADD FOREIGN KEY ("inspection_type_id") REFERENCES "cdw"."current"."inspection_type" ("inspection_type_id");

ALTER TABLE "cdw"."current"."assessment" ADD FOREIGN KEY ("parcel_id") REFERENCES "cdw"."current"."parcel" ("cdw_id");

ALTER TABLE "cdw"."current"."tax_delinquency" ADD FOREIGN KEY ("legal_entity_id") REFERENCES "cdw"."current"."legal_entity" ("legal_entity_id");

-- Native-reference preservation
ALTER TABLE "cdw"."current"."municipal_parcel_id_mapping" ADD FOREIGN KEY ("parcel_id") REFERENCES "cdw"."current"."parcel" ("cdw_id");

ALTER TABLE "cdw"."current"."municipal_parcel_id_mapping" ADD FOREIGN KEY ("municipality_id") REFERENCES "cdw"."current"."municipality" ("municipality_id");

-- Capture tables
ALTER TABLE "cdw"."current"."secondary_owner" ADD FOREIGN KEY ("parcel_id") REFERENCES "cdw"."current"."parcel" ("cdw_id");

ALTER TABLE "cdw"."current"."secondary_owner" ADD FOREIGN KEY ("owner_id") REFERENCES "cdw"."current"."legal_entity" ("legal_entity_id");
