CREATE SCHEMA IF NOT EXISTS current ;

CREATE TABLE IF NOT EXISTS "current"."parcel" (
  "parcel_id" int PRIMARY KEY,
  "county" varchar,
  "owner_id" int,
  "ward" int,
  "neighborhood" varchar,
  "zip_code" int,
  "census_block" int,
  "frontage" int,
  "zoning_class_id" int
);

CREATE TABLE IF NOT EXISTS "current"."building" (
  "building_id" int PRIMARY KEY,
  "parcel_id" int,
  "owner_id" int,
  "use_type_id" int,
  "sq_footage" int,
  "year_built" int
);

CREATE TABLE IF NOT EXISTS "current"."unit" (
  "unit_id" int PRIMARY KEY,
  "building_id" int,
  "owner_id" int,
  "use_type_id" int,
  "address_id" int,
  "ground_floor" bool,
  "stories" float,
  "windows_ac" int,
  "central_ac" int,
  "full_bath" int,
  "half_bath" int,
  "garage" int
);

CREATE TABLE IF NOT EXISTS "current"."legal_entity" (
  "legal_entity_id" int PRIMARY KEY,
  "name" varchar,
  "address_id" int
);

CREATE TABLE IF NOT EXISTS "current"."address" (
  "address_id" int PRIMARY KEY,
  "street_number" int,
  "street_name_prefix" varchar,
  "street_name" varchar,
  "street_name_suffix" varchar,
  "secondary_designator" varchar,
  "city" varchar,
  "state" varchar,
  "zip" int
);

CREATE TABLE IF NOT EXISTS "current"."permit" (
  "permit_id" int PRIMARY KEY,
  "permit_type_id" int,
  "application_date" date,
  "issue_date" date,
  "completion_date" date,
  "cancel_date" date,
  "description" varchar,
  "cost" float,
  "applicant_id" int,
  "contractor_id" int
);

CREATE TABLE IF NOT EXISTS "current"."service" (
  "service_id" int PRIMARY KEY,
  "parcel_id" int,
  "building_id" int,
  "unit_id" int,
  "owner_id" int,
  "division_id" int,
  "service_date" date,
  "service_type_id" int,
  "fee" float8
);

CREATE TABLE IF NOT EXISTS "current"."condemnation" (
  "condemnation_id" int PRIMARY KEY,
  "inspection_id" int,
  "letter_date" date,
  "condemnation_type" varchar,
  "status" varchar
);

CREATE TABLE IF NOT EXISTS "current"."inspection" (
  "inspection_id" int PRIMARY KEY,
  "building_id" int,
  "inspection_date" date,
  "completion_date" date,
  "inspection_type_id" int,
  "number_of_violations" int
);

CREATE TABLE IF NOT EXISTS "current"."assessment" (
  "assessment_id" int PRIMARY KEY,
  "parcel_id" int,
  "assessment_date" date,
  "assessment_amount" float,
  "land_value" float,
  "improvement_value" float
);

CREATE TABLE IF NOT EXISTS "current"."tax_delinquency" (
  "tax_delinquency_id" int PRIMARY KEY,
  "legal_entity_id" int,
  "last_paid_year" int,
  "amount_delinquent" float
);

CREATE TABLE IF NOT EXISTS "current"."municipality" (
  "municipality_id" int PRIMARY KEY,
  "municipality" varchar
);

CREATE TABLE IF NOT EXISTS "current"."division" (
  "division_id" int PRIMARY KEY,
  "division" varchar
);

CREATE TABLE IF NOT EXISTS "current"."service_type" (
  "service_type_id" int PRIMARY KEY,
  "service_type" varchar
);

CREATE TABLE IF NOT EXISTS "current"."zoning_class" (
  "zoning_class_id" int PRIMARY KEY,
  "zoning_class" varchar
);

CREATE TABLE IF NOT EXISTS "current"."municipal_parcel_id_mapping" (
  "parcel_id" int PRIMARY KEY,
  "municpal_parcel_id" varchar,
  "municipality_id" int
);

CREATE TABLE IF NOT EXISTS "current"."use_type" (
  "use_type_id" int PRIMARY KEY,
  "use_type" int
);

CREATE TABLE IF NOT EXISTS "current"."permit_type" (
  "permit_type_id" int PRIMARY KEY,
  "permit_type" varchar
);

CREATE TABLE IF NOT EXISTS "current"."inspection_type" (
  "inspection_type_id" int PRIMARY KEY,
  "inspection_type" varchar
);

COMMENT ON COLUMN "current"."parcel"."parcel_id" IS 'REDB identifier, not municapal identifier (handle)';

ALTER TABLE "current"."parcel" ADD FOREIGN KEY ("owner_id") REFERENCES "current"."legal_entity" ("legal_entity_id");

ALTER TABLE "current"."parcel" ADD FOREIGN KEY ("zoning_class_id") REFERENCES "current"."zoning_class" ("zoning_class_id");

ALTER TABLE "current"."building" ADD FOREIGN KEY ("parcel_id") REFERENCES "current"."parcel" ("parcel_id");

ALTER TABLE "current"."building" ADD FOREIGN KEY ("owner_id") REFERENCES "current"."legal_entity" ("legal_entity_id");

ALTER TABLE "current"."building" ADD FOREIGN KEY ("use_type_id") REFERENCES "current"."use_type" ("use_type_id");

ALTER TABLE "current"."unit" ADD FOREIGN KEY ("building_id") REFERENCES "current"."building" ("building_id");

ALTER TABLE "current"."unit" ADD FOREIGN KEY ("owner_id") REFERENCES "current"."legal_entity" ("legal_entity_id");

ALTER TABLE "current"."unit" ADD FOREIGN KEY ("use_type_id") REFERENCES "current"."use_type" ("use_type_id");

ALTER TABLE "current"."unit" ADD FOREIGN KEY ("address_id") REFERENCES "current"."address" ("address_id");

ALTER TABLE "current"."legal_entity" ADD FOREIGN KEY ("address_id") REFERENCES "current"."address" ("address_id");

ALTER TABLE "current"."permit" ADD FOREIGN KEY ("permit_type_id") REFERENCES "current"."permit_type" ("permit_type_id");

ALTER TABLE "current"."permit" ADD FOREIGN KEY ("applicant_id") REFERENCES "current"."legal_entity" ("legal_entity_id");

ALTER TABLE "current"."permit" ADD FOREIGN KEY ("contractor_id") REFERENCES "current"."legal_entity" ("legal_entity_id");

ALTER TABLE "current"."service" ADD FOREIGN KEY ("parcel_id") REFERENCES "current"."parcel" ("parcel_id");

ALTER TABLE "current"."service" ADD FOREIGN KEY ("building_id") REFERENCES "current"."building" ("building_id");

ALTER TABLE "current"."service" ADD FOREIGN KEY ("unit_id") REFERENCES "current"."unit" ("unit_id");

ALTER TABLE "current"."service" ADD FOREIGN KEY ("owner_id") REFERENCES "current"."legal_entity" ("legal_entity_id");

ALTER TABLE "current"."service" ADD FOREIGN KEY ("division_id") REFERENCES "current"."division" ("division_id");

ALTER TABLE "current"."service" ADD FOREIGN KEY ("service_type_id") REFERENCES "current"."service_type" ("service_type_id");

ALTER TABLE "current"."condemnation" ADD FOREIGN KEY ("inspection_id") REFERENCES "current"."inspection" ("inspection_id");

ALTER TABLE "current"."inspection" ADD FOREIGN KEY ("building_id") REFERENCES "current"."building" ("building_id");

ALTER TABLE "current"."inspection" ADD FOREIGN KEY ("inspection_type_id") REFERENCES "current"."inspection_type" ("inspection_type_id");

ALTER TABLE "current"."assessment" ADD FOREIGN KEY ("parcel_id") REFERENCES "current"."parcel" ("parcel_id");

ALTER TABLE "current"."tax_delinquency" ADD FOREIGN KEY ("legal_entity_id") REFERENCES "current"."legal_entity" ("legal_entity_id");

ALTER TABLE "current"."municipal_parcel_id_mapping" ADD FOREIGN KEY ("parcel_id") REFERENCES "current"."parcel" ("parcel_id");
