--------------------------------------------------
-- add encounter info for voyages of Russian trawler VOIs for May 2025 NPG patrol
--------------------------------------------------

# set last day of time window of interest for capping events
CREATE TEMP FUNCTION  end_day() AS (TIMESTAMP('2025-05-29 23:59:59 UTC'));
## set active period of interest
CREATE TEMP FUNCTION  active_period_start() AS (TIMESTAMP('2025-03-01 00:00:00 UTC'));
CREATE TEMP FUNCTION  active_period_end() AS (TIMESTAMP('2025-06-01 23:59:59 UTC'));

WITH
----------------------------------------------------------
-- pull relevant voyage info from starting table
----------------------------------------------------------
voyages AS (
  SELECT
    vessel_id,
    ssvid,
    trip_id,
    trip_start,
    -- trip_end
    IFNULL(trip_end, end_day()) AS trip_end,

  FROM `scratch_joef.DFO_VOI_2025_voyages`
  ),

----------------------------------------------------------
-- pull encounters and associated info for voyages
----------------------------------------------------------
encounters AS (
  SELECT
  DISTINCT
    "Encounter" AS event_type,
    enc.event_id,
    voyages.ssvid,
    voyages.trip_id,
    enc.event_start,
    enc.event_end,
    enc.event_duration_hrs,
    ROUND(enc.lat_mean, 2) AS lat_mean,
    ROUND(enc.lon_mean, 2) AS lon_mean,
    ROUND(CAST(enc.distance_km AS numeric), 1) AS distance_km,
    ROUND(CAST(enc.speed_knots AS numeric), 1) AS speed_knots,
    eez,
    enc.major_fao,
    enc.high_seas,
    enc.rfmo,
    ROUND(enc.start_distance_from_shore_km, 1) AS start_distance_from_shore_km,
    enc.encountered_ssvid,
    enc.encountered_vessel_id
  FROM(
    SELECT
      event_id,
      vessel_id,
      ## extract information on vessel ssvid and vessel type
      JSON_EXTRACT_SCALAR(event_vessels, "$[0].ssvid") as ssvid,
      JSON_EXTRACT_SCALAR(event_vessels, "$[1].ssvid") as encountered_ssvid,
      JSON_EXTRACT_SCALAR(event_vessels, "$[1].id") as encountered_vessel_id,
      event_start,
      event_end,
      ROUND(TIMESTAMP_DIFF(event_end, event_start, minute) / 60, 3) AS event_duration_hrs,
      lat_mean,
      lon_mean,
      JSON_EXTRACT_SCALAR(event_info, "$.median_distance_km") as distance_km,
      JSON_EXTRACT_SCALAR(event_info, "$.median_speed_knots") as speed_knots,
      ## pull out event regions
      -- ARRAY_TO_STRING(regions_mean_position.eez, ", ") AS eez,
      regions_mean_position.eez AS eez,
      ARRAY_TO_STRING(regions_mean_position.major_fao, ", ") AS major_fao,
      ARRAY_TO_STRING(regions_mean_position.high_seas, ", ") AS high_seas,
      ARRAY_TO_STRING(regions_mean_position.rfmo, ", ") AS rfmo,
      -- regions_mean_position.rfmo AS rfmo,
      start_distance_from_shore_km,
      FROM `pipe_ais_v3_published.product_events_encounter`
      ) enc
    INNER JOIN voyages
      ON
    enc.event_start BETWEEN voyages.trip_start AND voyages.trip_end
    AND
     voyages.vessel_id = enc.vessel_id
    WHERE enc.event_start >= active_period_start()
    ),

----------------------------------------------------------
-- for encounters, add vessel info for encountered vessels
----------------------------------------------------------
encounter_v_info AS(
  SELECT
    events.*,
    vi.shipname AS encountered_shipname,
    vi.callsign AS encountered_callsign,
    vi.imo AS encountered_imo,
    vi.vessel_iso3 AS encountered_flag,
    vi.vessel_class_best AS encountered_vessel_class,
    vi.geartype AS encountered_geartype,
  FROM(
    SELECT
      *,
      EXTRACT(year FROM event_end) AS year
    FROM encounters) events
    LEFT JOIN (
      SELECT
        vessel_id,
        ssvid,
        year,
        shipname,
        callsign,
        imo,
        IFNULL(IFNULL(gfw_best_flag, core_flag), mmsi_flag) AS vessel_iso3,
        prod_shiptype AS vessel_class_best,
        prod_geartype AS geartype
      FROM
        `pipe_ais_v3_published.product_vessel_info_summary`) vi
      ON
        events.encountered_vessel_id = vi.vessel_id AND events.year = vi.year),

----------------------------------------------------------
-- create column of eez isos that allows for joint/disputed areas w/ >1 eez per code
----------------------------------------------------------
eez_names AS (
  SELECT
    eez_id,
    CASE
      WHEN eez3 IS NOT NULL AND eez2 IS NOT NULL THEN CONCAT(eez1, "/", eez2, "/", eez3)
      WHEN eez2 IS NOT NULL THEN CONCAT(eez1, "/", eez2)
      ELSE eez1 END AS eez_name
  FROM(
    SELECT
      CAST(eez_id AS STRING) AS eez_id,
      sovereign1_iso3 AS eez1,
      CASE WHEN sovereign2_iso3 = "NA" THEN null ELSE sovereign2_iso3 END AS eez2,
      CASE WHEN sovereign3_iso3 = "NA" THEN null ELSE sovereign3_iso3 END AS eez3
      -- reporting_name AS eez_name
    FROM `gfw_research.eez_info`)),

----------------------------------------------------------
-- join eez info to codes in event table
----------------------------------------------------------
add_eez_info AS(
  SELECT
    encs.event_type,
    encs.event_id,
    encs.ssvid,
    encs.trip_id,
    encs.event_start,
    encs.event_end,
    encs.event_duration_hrs,
    encs.lat_mean,
    encs.lon_mean,
    encs.distance_km,
    encs.speed_knots,
    -- encs.eez,
    -- eez_id,
    ARRAY_TO_STRING(ARRAY_AGG(eez_name ORDER BY eez_name), ", ") AS eez,
    encs.major_fao,
    encs.high_seas,
    encs.rfmo,
    encs.start_distance_from_shore_km,
    encs.encountered_ssvid,
    encs.encountered_vessel_id,
    encs.year,
    encs.encountered_shipname,
    encs.encountered_callsign,
    encs.encountered_imo,
    encs.encountered_flag,
    encs.encountered_vessel_class,
    encs.encountered_geartype
  FROM encounter_v_info AS encs
  LEFT JOIN UNNEST (eez) AS eez_id
  LEFT JOIN eez_names
  USING (eez_id)
  GROUP BY
    event_type,
    event_id,
    ssvid,
    trip_id,
    event_start,
    event_end,
    event_duration_hrs,
    lat_mean,
    lon_mean,
    distance_km,
    speed_knots,
    major_fao,
    high_seas,
    rfmo,
    start_distance_from_shore_km,
    encountered_ssvid,
    encountered_vessel_id,
    year,
    encountered_shipname,
    encountered_callsign,
    encountered_imo,
    encountered_flag,
    encountered_vessel_class,
    encountered_geartype
)

----------------------------------------------------------
-- split rfmo strings into arrays, sort, then re-concatenate as strings
----------------------------------------------------------
SELECT
  event_type,
    event_id,
    ssvid,
    trip_id,
    event_start,
    event_end,
    event_duration_hrs,
    lat_mean,
    lon_mean,
    distance_km,
    speed_knots,
    eez,
    major_fao,
    CASE WHEN high_seas = "" THEN "N" ELSE "Y" END AS high_seas,
    ARRAY_TO_STRING(ARRAY(select rfmos from UNNEST(rfmo) rfmos ORDER BY rfmos), ", ") AS rfmo,
    start_distance_from_shore_km,
    encountered_ssvid,
    encountered_vessel_id,
    year,
    encountered_shipname,
    encountered_callsign,
    encountered_imo,
    encountered_flag,
    encountered_vessel_class,
    encountered_geartype
  FROM(
  SELECT
    event_type,
    event_id,
    ssvid,
    trip_id,
    event_start,
    event_end,
    event_duration_hrs,
    lat_mean,
    lon_mean,
    distance_km,
    speed_knots,
    eez,
    major_fao,
    high_seas,
    SPLIT(rfmo,', ') AS rfmo,
    start_distance_from_shore_km,
    encountered_ssvid,
    encountered_vessel_id,
    year,
    encountered_shipname,
    encountered_callsign,
    encountered_imo,
    encountered_flag,
    encountered_vessel_class,
    encountered_geartype
  FROM add_eez_info
  WHERE
    encountered_vessel_class != 'gear' AND encountered_geartype != 'gear'

  )

/*


*/
