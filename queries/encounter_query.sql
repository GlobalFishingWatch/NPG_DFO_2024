
--- this query could be used to get raw encounter information directly from voyage query.
-- Note however the alternative workflow is to take the voyage output and use in the
-- encounter_info_query as done for port profiles


--------------------------------------
  -- Query to identify fishing and carrier vessels in the NPG AOI.
  -- using geojson of npfc AOI to pull active vessels

  -- this version also fixes gear in encounters vs the old version

## set time frame of interest for voyages (i.e., voyages must start after this date to be included)
CREATE TEMP FUNCTION  voyage_start_date() AS (TIMESTAMP('2023-01-01 00:00:00 UTC'));

CREATE TEMP FUNCTION  start_year() AS (2023);
CREATE TEMP FUNCTION  end_year() AS (2024);

## set active period of interest
CREATE TEMP FUNCTION  active_period_start() AS (TIMESTAMP('2024-05-01 00:00:00 UTC'));
CREATE TEMP FUNCTION  active_period_end() AS (TIMESTAMP('2024-06-02 23:59:59 UTC'));

## set the current day to calculate voyage duration up to (GFW) real time AND to ensure no voyage starts after it..
CREATE TEMP FUNCTION  current_day() AS (TIMESTAMP('2024-06-02 23:59:59 UTC'));

-- CREATE TABLE `world-fishing-827.scratch_joef.NPFC_vessels_May1-Jun2` AS
CREATE TABLE `world-fishing-827.scratch_joef.NPFC_encounters_May1-Jun2` AS

--------------------------------------
WITH

----------------------------------------------------------
-- Define list of VOIs
----------------------------------------------------------

----------------------------------------------------------
# Create a geometry object for the polygon. Can use either GeoJSON or WKT input.
# copied appropriate GeoJSON text from R variable to match Tyler's code (from GFW map)
----------------------------------------------------------
AOI as (
SELECT
  # GeoJSON
  ST_GEOGFROMGEOJSON('{ "type": "Polygon", "coordinates":  [[[144.85113,35.0],[160.0,35.0],[160.0,45.0],[156.0,45.0],[155.4599716,44.60921],[154.9694765,44.2967314],[154.4789814,44.0356874],[154.051776,43.834391],[153.6509414,43.7087113],[153.1604463,43.3569375],[152.4589856,42.9722481],[151.8419111,42.6782647],[151.3936092,42.5113129],[151.0296935,42.417936],[150.6288588,41.8940238],[149.9379464,41.3499436],[149.2681305,40.9607829],[148.6035888,40.6253792],[148.123642,40.4128843],[146.8420258,39.9656675],[146.3726273,39.5846403],[146.2354996,38.7751656],[146.0192599,38.1971984],[145.4707493,37.010571],[145.0646404,36.4314056],[144.9433352,36.0784048],[144.9644318,35.5995444],[144.9380611,35.2987918],[144.85113,35.0]]] } ')
   AS NPFC_geojson
),

----------------------------------------------------------
-- all vessels active in AOI during period of interest from messages table
----------------------------------------------------------
active_vessel_ids AS (
  SELECT DISTINCT
    vessel_id,
    ssvid,
    timestamp
  FROM `world-fishing-827.pipe_ais_v3_published.messages`

    WHERE timestamp >= active_period_start() AND timestamp <= active_period_end()
  # Filter to polygon using ST_CONTAINS.
  # You need to create a geometry object for each position using ST_GEOPOINT
  AND ST_CONTAINS((SELECT NPFC_geojson FROM AOI), ST_GEOGPOINT(lon, lat))
  ),

----------------------------------------------------------
-- pull initial vessel info / format for rest of query
----------------------------------------------------------
AOI_vessels AS(
 SELECT DISTINCT
   vessel_id,
   year,
   IFNULL(IFNULL(gfw_best_flag, core_flag), mmsi_flag) AS vessel_iso3,
   'inAOI' AS vessel_class,
   prod_geartype AS gear_type
  FROM
    `pipe_ais_v3_published.product_vessel_info_summary_v20240501`
  WHERE
   year = 2024
   AND vessel_id IN (SELECT vessel_id FROM active_vessel_ids)
  ),

----------------------------------------------------------
-- voyages for all identified vessels with ongoing voyages in the AOI
----------------------------------------------------------
  voyages AS (
    SELECT
      *
    FROM (
      SELECT
        *,
      FROM
        `pipe_ais_v3_published.voyages_c3`
      WHERE
        (trip_start >= voyage_start_date() OR trip_start IS NULL)
        AND trip_end IS NULL
        AND trip_start <= current_day()
        )
    INNER JOIN AOI_vessels
    USING
      (vessel_id)
      ),

--------------------------------------
-- Anchorage names
--------------------------------------
  anchorage_names AS (
  SELECT
    s2id,
    label,
    iso3
  FROM
    `anchorages.named_anchorages_v20240117`
    ),

--------------------------------------
-- Add names to voyages (start and end)
--------------------------------------
  named_voyages AS (
  SELECT
    * EXCEPT(s2id, label, iso3),
    c.label AS end_label,
    c.iso3 AS end_iso3
  FROM (
    SELECT
      * EXCEPT(s2id, label, iso3),
      b.label AS start_label,
      b.iso3 AS start_iso3
    FROM
      voyages
    LEFT JOIN
      anchorage_names b
    ON
      trip_start_anchorage_id = s2id)
  LEFT JOIN
    anchorage_names c
  ON
    trip_end_anchorage_id = s2id),

-------------------------------------------------------------------
-- The following bit of code is intended to splice together voyages
-- that pass through the Panama Canal into a single voyage, so as not to inflate assigned
-- port visits to Panama when the vessel is only using the Canal for transit
-------------------------------------------------------------------

-- **** consider whether to add same logic for other straits or canals

------------------------------------------------
-- anchorage ids that represent the Panama Canal

-- JF - note the named_anchorages table was updated without
-- the Panama Canal sublabels so these are saved in a
-- separate table for now
------------------------------------------------
  panama_canal_ids AS (
    SELECT s2id AS anchorage_id
    FROM `world-fishing-827.anchorages.panama_canal_v20231004`
    WHERE sublabel="PANAMA CANAL" -- not strictly necessary as this table filtered to canal already..
  ),

-------------------------------------------------------------------
-- Mark whether start anchorage or end anchorage is in Panama canal
-------------------------------------------------------------------
  is_end_port_panama AS (
    SELECT
    ssvid,
    vessel_id,
    vessel_iso3,
    -- class_confidence,
    vessel_class,
    gear_type,
    trip_id,
    trip_start,
    trip_end,
    start_iso3,
    start_label,
    end_iso3,
    end_label,
    trip_start_confidence,
    trip_end_confidence,
    trip_start_visit_id,
    trip_end_visit_id,
    trip_start_anchorage_id,
    trip_end_anchorage_id,
    IF (trip_start_anchorage_id IN (
      SELECT anchorage_id FROM panama_canal_ids),
      TRUE, FALSE) current_start_is_panama,
    IF (trip_end_anchorage_id IN (
      SELECT anchorage_id FROM panama_canal_ids),
      TRUE, FALSE) current_end_is_panama,
    FROM named_voyages
  ),

------------------------------------------------
-- Add information about
-- whether previous and next ports are in Panama
------------------------------------------------
  add_prev_next_port AS (
    SELECT
    *,
    IFNULL (
      LAG (trip_start, 1) OVER (
        PARTITION BY ssvid
        ORDER BY trip_start ASC ),
      TIMESTAMP ("2000-01-01") ) AS prev_trip_start,
    -- note as structured the prev trip id will be null for each vessels' first voyage in the time period
    LAG (trip_id, 1) OVER (
      PARTITION BY ssvid
      ORDER BY trip_start ASC ) AS prev_trip_id,
    IFNULL (
      LEAD (trip_end, 1) OVER (
        PARTITION BY ssvid
        ORDER BY trip_start ASC ),
      TIMESTAMP ("2100-01-01") ) AS next_trip_end,
    LAG (current_end_is_panama, 1) OVER (
      PARTITION BY ssvid
      ORDER BY trip_start ASC ) AS prev_end_is_panama,
    LEAD (current_end_is_panama, 1) OVER (
      PARTITION BY ssvid
      ORDER BY trip_start ASC ) AS next_end_is_panama,
    LAG (current_start_is_panama, 1) OVER(
      PARTITION BY ssvid
      ORDER BY trip_start ASC ) AS prev_start_is_panama,
    LEAD (current_start_is_panama, 1) OVER(
      PARTITION BY ssvid
      ORDER BY trip_start ASC ) AS next_start_is_panama,
    FROM is_end_port_panama
  ),

---------------------------------------------------------------------------------
-- Mark the start and end of the block. The start of the block is the anchorage
-- just before Panama canal, and the end of the block is the anchorage just after
-- Panama canal (all consecutive trips within Panama canal will be ignored later).
-- If there is no Panama canal involved in a trip, the start/end of the block are
-- the trip start/end of that trip.

-- note there are some trips in which the arrival port is diff than the next departure
-- (ie one is a canal port and the other not), which makes just classifying based on
-- current start and end fail - this query requires both current and prev start/end
---------------------------------------------------------------------------------
  block_start_end AS (
    SELECT
    *,
          IF (current_start_is_panama AND prev_end_is_panama, NULL, trip_start) AS block_start,
          IF (current_end_is_panama AND next_start_is_panama, NULL, trip_end) AS block_end
    FROM add_prev_next_port
  ),

-------------------------------------------
-- Find the closest non-Panama ports
-- by looking ahead and back of the records
-------------------------------------------
  look_back_and_ahead AS (
    SELECT
    * EXCEPT(block_start, block_end),
    LAST_VALUE (block_start IGNORE NULLS) OVER (
      PARTITION BY ssvid
      ORDER BY trip_start
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS block_start,
    FIRST_VALUE (block_end IGNORE NULLS) OVER (
      PARTITION BY ssvid
      ORDER BY trip_start
      ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS block_end
    FROM block_start_end
  ),

-------------------------------------------------------------------
-- Within a block, all trips will have the same information
-- about their block (start / end of the block, anchorage start/end)
-------------------------------------------------------------------
  blocks_to_be_collapsed_down AS (
    SELECT
    ssvid,
    block_start,
    block_end,
    FIRST_VALUE (vessel_id) OVER (
      PARTITION BY block_start, block_end, ssvid
      ORDER BY trip_start ASC) AS vessel_id,
    FIRST_VALUE (vessel_iso3) OVER (
      PARTITION BY block_start, block_end, ssvid
      ORDER BY trip_start ASC) AS vessel_iso3,
    -- FIRST_VALUE (class_confidence) OVER (
    --   PARTITION BY block_start, block_end, ssvid
    --   ORDER BY trip_start ASC) AS class_confidence,
    FIRST_VALUE (vessel_class) OVER (
      PARTITION BY block_start, block_end, ssvid
      ORDER BY trip_start ASC) AS vessel_class,
    FIRST_VALUE (gear_type) OVER (
      PARTITION BY block_start, block_end, ssvid
      ORDER BY trip_start ASC) AS gear_type,
    FIRST_VALUE (trip_id) OVER (
      PARTITION BY block_start, block_end, ssvid
      ORDER BY trip_start ASC) AS trip_id,
    FIRST_VALUE (start_iso3) OVER (
      PARTITION BY block_start, block_end, ssvid
      ORDER BY trip_start ASC) AS start_iso3,
    FIRST_VALUE (start_label) OVER (
      PARTITION BY block_start, block_end, ssvid
      ORDER BY trip_start ASC) AS start_label,
    FIRST_VALUE (end_iso3) OVER (
      PARTITION BY block_start, block_end, ssvid
      ORDER BY trip_end DESC) AS end_iso3,
    FIRST_VALUE (end_label) OVER (
      PARTITION BY block_start, block_end, ssvid
      ORDER BY trip_end DESC) AS end_label,
    FIRST_VALUE (trip_start_visit_id) OVER (
      PARTITION BY block_start, block_end, ssvid
      ORDER BY trip_start ASC) AS trip_start_visit_id,
    FIRST_VALUE (trip_end_visit_id) OVER (
      PARTITION BY block_start, block_end, ssvid
      ORDER BY trip_end DESC) AS trip_end_visit_id,
    FIRST_VALUE (trip_start_anchorage_id) OVER (
      PARTITION BY block_start, block_end, ssvid
      ORDER BY trip_start ASC) AS trip_start_anchorage_id,
    FIRST_VALUE (trip_end_anchorage_id) OVER (
      PARTITION BY block_start, block_end, ssvid
      ORDER BY trip_end DESC) AS trip_end_anchorage_id,

    FIRST_VALUE (trip_start_confidence) OVER (
      PARTITION BY block_start, block_end, ssvid
      ORDER BY trip_start ASC) AS trip_start_confidence,
    FIRST_VALUE (trip_end_confidence) OVER (
      PARTITION BY block_start, block_end, ssvid
      ORDER BY trip_end DESC) AS trip_end_confidence,
    FROM look_back_and_ahead
  ),

---------------------------------------------------------------------
-- Blocks get collapsed down to one row, which means a block of trips
-- becomes a complete trip
---------------------------------------------------------------------
  updated_pan_voyages AS (
    SELECT
      ssvid,
      vessel_id,
      vessel_iso3,
      -- class_confidence,
      vessel_class,
      gear_type,
      trip_id,
      block_start AS trip_start,
      -- block_end AS trip_end,
      start_iso3,
      start_label,
      -- end_iso3,
      -- end_label,
      trip_start_visit_id,
      -- trip_end_visit_id,
      trip_start_anchorage_id,
      -- trip_end_anchorage_id,
      trip_start_confidence,
      -- trip_end_confidence,
      CASE -- adding flag if voyages is collapsed bc of PAN crossing
        WHEN count(*) > 1 THEN 1
        WHEN count(*) = 1 THEN 0
        END AS pan_crossing
    FROM blocks_to_be_collapsed_down
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
  ),

--------------------------------------
-- Identify how many encounters occurred on each voyage
--------------------------------------
-- pipe 3
num_encounters AS (
  SELECT *
  --   vessel_id,
  --   trip_id,
  --   COUNT(*) AS num_encounters
  FROM (
    SELECT
      vessel_id,
      JSON_EXTRACT_SCALAR(event_vessels, "$[0].ssvid") as ssvid,
      JSON_EXTRACT_SCALAR(event_vessels, "$[0].type") as product_shiptype,
      -- ## encountered vessel information
      JSON_EXTRACT_SCALAR(event_vessels, "$[1].type") as enc_product_shiptype,
      JSON_EXTRACT_SCALAR(event_vessels, "$[1].id") as enc_product_vessel_id,
      JSON_EXTRACT_SCALAR(event_vessels, "$[1].ssvid") as enc_product_ssvid,
      regions_mean_position.eez AS eez,
      regions_mean_position.high_seas AS high_seas,
      regions_mean_position.rfmo,
      event_start,
      event_end,
      lat_mean,
      lon_mean,
      start_distance_from_shore_km,
    FROM `pipe_ais_v3_published.product_events_encounter`)
    INNER JOIN (
      SELECT
        vessel_id,
        trip_id,
        trip_start
      FROM
        updated_pan_voyages) voyages
    USING (vessel_id)
      WHERE event_start >= trip_start
      AND  product_shiptype != 'gear' AND enc_product_shiptype != 'gear'
    )


    select * from num_encounters
