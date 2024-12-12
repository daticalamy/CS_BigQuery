--liquibase formatted sql
--changeset amy_smith:02

CREATE or replace TABLE `TEST_SET_01.population_by_zip_2010`
(
  geo_id STRING OPTIONS(description="Geo code"),
  zipcode STRING NOT NULL OPTIONS(description="Five digit ZIP Code Tabulation Area Census Code")
)
OPTIONS(
  labels=[("pii", "")]
);
--rollback drop table TEST_SET_01.population_by_zip_2010


--changeset amy_smith:03
CREATE or replace TABLE `TEST_SET_01.population_by_zip_3333`
(
  geo_id STRING OPTIONS(description="Geo code"),
  zipcode STRING NOT NULL OPTIONS(description="Five digit ZIP Code Tabulation Area Census Code")
)
OPTIONS(
  labels=[("pii", "")]
);

CREATE or replace VIEW TEST_SET_01.population_by_zip_3333_vw(geo_id, zipcode) AS (
  SELECT
    geo_id,
    zipcode
  FROM
    TEST_SET_01.population_by_zip_3333
  ORDER BY
    geo_id DESC
);

CREATE or replace VIEW TEST_SET_01.population_by_zip_3333_s(geo_id, zipcode) AS (
  SELECT
    geo_id,
    zipcode
  FROM
    TEST_SET_01.population_by_zip_3333
  ORDER BY
    geo_id DESC
);
--rollback drop view TEST_SET_01.population_by_zip_3333_s; drop view TEST_SET_01.population_by_zip_3333_vw; drop table TEST_SET_01.population_by_zip_3333; 


--changeset amy_smith:04
CREATE or replace TABLE `TEST_SET_01.population_by_zip_4444`
(
  geo_id STRING OPTIONS(description="Geo code"),
  zipcode STRING NOT NULL OPTIONS(description="Five digit ZIP Code Tabulation Area Census Code")
)
OPTIONS(
  labels=[("pii", "")]
);

CREATE VIEW TEST_SET_01.population_by_zip_4444_x AS (
  SELECT
    geo_id,
    zipcode
  FROM
    TEST_SET_01.population_by_zip_4444
  ORDER BY
    geo_id DESC
);

CREATE VIEW TEST_SET_01.population_by_zip_444_x(geo_id, zipcode) AS (
  SELECT
    geo_id,
    zipcode
  FROM
    TEST_SET_01.population_by_zip_4444
  ORDER BY
    geo_id DESC
);
--rollback drop view TEST_SET_01.population_by_zip_444_s; drop view TEST_SET_01.population_by_zip_4444_s; drop table TEST_SET_01.population_by_zip_4444; 