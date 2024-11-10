--------------------------------------
  -- Query to identify fishing and carrier vessels in the NPG AOI.
  -- using geojson of npfc AOI to pull active vessels

  -- this version also fixes gear in encounters vs the old version

## set time frame of interest for voyages (i.e., voyages must start after this date to be included)
CREATE TEMP FUNCTION  voyage_start_date() AS (TIMESTAMP('2023-01-01 00:00:00 UTC'));
CREATE TEMP FUNCTION  start_year() AS (2021);
CREATE TEMP FUNCTION  end_year() AS (2023);

## set active period of interest
CREATE TEMP FUNCTION  active_period_start() AS (TIMESTAMP('2024-09-01 00:00:00 UTC'));
CREATE TEMP FUNCTION  active_period_end() AS (TIMESTAMP('2024-11-08 23:59:59 UTC'));

## set the current/ending day to calculate voyage duration up to (GFW) real time AND to ensure no voyage starts after it (also need for events bounding)..
CREATE TEMP FUNCTION  end_day() AS (TIMESTAMP('2024-11-06 23:59:59 UTC'));

CREATE or replace TABLE `world-fishing-827.scratch_joef.PHL_vessels_sep01-08nov` AS

--------------------------------------
WITH

----------------------------------------------------------
-- Define list of VOIs
----------------------------------------------------------


AOI_vessels AS(
   SELECT DISTINCT
     vessel_id,
    --  ssvid,
     year,
     IFNULL(IFNULL(gfw_best_flag, core_flag), mmsi_flag) AS vessel_iso3,
     'inAOI' AS origin_list,
     prod_geartype AS gear_type
    FROM
     `pipe_ais_v3_published.product_vessel_info_summary_v20241001`
    WHERE
     year = 2024
     AND ssvid IN (
          "416001007","416231500","416231500","416004389","416002526","416002491","416003677","416000373","416000373","416000853","416002633","416003466","416237800","416002973","416002973","416002989","431635000","416000447","416000947","416004098","416000702","416004299","416003561","416003814","431680124","416001774","412468636","416002934","416001567","416232800","431680680","431680230","416002441","525900560","416076700","416002631","416154700","416171600","431301296","201708106","412706139","416000469","416000699","416002232","416002343","416002415","416002415","416002492","416003793","416004158","416004158","416004442","416099800","416218500","416242800","431002571","431010018","431011377","431200460","431500880","431600420","431680156","431680580","431680790","525700988","525900848","525201323","431015619","416000202","416000445","416000737","416000754","416000754","416003636","416196500","416003187", "431680630", "416009187", "416001943"
     )
    ),

----------------------------------------------------------
-- voyages for all identified vessels with ongoing voyages in the AOI
----------------------------------------------------------
  raw_voyages AS (
    SELECT
      *
    FROM (
      SELECT
        ssvid,
        vessel_id,
        trip_start,
        trip_start_anchorage_id,
        trip_start_visit_id,
        trip_start_confidence,
        trip_id,
      FROM
        `pipe_ais_v3_published.voyages_c3`
      WHERE
        (trip_start >= voyage_start_date() OR trip_start IS NULL)
        AND trip_end IS NULL
        AND trip_start <= end_day()
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
      b.label AS start_label,
      b.iso3 AS start_iso3
    FROM
      raw_voyages
    LEFT JOIN
      anchorage_names b
    ON
      trip_start_anchorage_id = s2id
      ),

--------------------------------------
-- Identify how many encounters during period of interest
--------------------------------------
 num_encounters AS (
    SELECT
      vessel_id,
      trip_id,
      COUNT(*) AS num_encounters
    FROM (
      SELECT
        vessel_id,
        event_start,
        event_end,
        JSON_EXTRACT_SCALAR(event_vessels, "$[0].type") as product_shiptype,
        -- ## encountered vessel information
        JSON_EXTRACT_SCALAR(event_vessels, "$[1].type") as enc_product_shiptype,
        JSON_EXTRACT_SCALAR(event_vessels, "$[1].id") as enc_product_vessel_id,
        JSON_EXTRACT_SCALAR(event_vessels, "$[1].ssvid") as enc_product_ssvid,
        start_distance_from_port_km
      FROM `world-fishing-827.pipe_ais_v3_published.product_events_encounter`) enc
    INNER JOIN (
      SELECT
        vessel_id,
        trip_id,
        trip_start
      FROM
        raw_voyages)
    USING
      (vessel_id)
    WHERE
      event_start BETWEEN trip_start AND end_day()
          AND  product_shiptype != 'gear' AND enc_product_shiptype != 'gear'
          AND start_distance_from_port_km > 10
      GROUP BY
        vessel_id, trip_id
    ),

--------------------------------------
-- Identify how many loitering events occurred on each voyage
--------------------------------------
  num_loitering AS (
    SELECT
      vessel_id,
      trip_id,
      COUNT(*) AS num_loitering
    FROM (
      SELECT
        vessel_id,
        event_start
      FROM
        `pipe_ais_v3_published.product_events_loitering`
      WHERE
        seg_id IN (
        SELECT
          seg_id
        FROM
          `pipe_ais_v3_published.segs_activity`
        WHERE
          good_seg IS TRUE
          AND overlapping_and_short IS FALSE)
          AND SAFE_CAST(JSON_QUERY(event_info,"$.avg_distance_from_shore_km") AS FLOAT64) > 37.04 -- to match 20 nm rule used in map
          AND SAFE_CAST(JSON_QUERY(event_info,"$.loitering_hours") AS FLOAT64) > 2
          AND SAFE_CAST(JSON_QUERY(event_info,"$.avg_speed_knots") AS FLOAT64) < 2) a
    INNER JOIN (
      SELECT
        vessel_id,
        trip_id,
        trip_start
      FROM
        raw_voyages) b
    USING
      (vessel_id)
    WHERE event_start BETWEEN trip_start AND end_day()
    GROUP BY
      vessel_id, trip_id
      ),

--------------------------------------
-- Identify how many fishing events occurred on each voyage
--------------------------------------
  num_fishing AS(
    SELECT
      vessel_id,
      trip_id,
      COUNT(*) AS num_fishing
    FROM (
      SELECT
        vessel_id,
        event_start
      FROM
        `pipe_ais_v3_published.product_events_fishing`) a
    INNER JOIN (
      SELECT
        vessel_id,
        trip_id,
        trip_start
      FROM
        raw_voyages)
    USING
      (vessel_id)
    WHERE event_start BETWEEN trip_start AND end_day()
    GROUP BY
      vessel_id, trip_id
      ),

--------------------------------------
-- Identify how many gaps occurred on each voyage
--------------------------------------
  num_gaps AS(
    SELECT
      vessel_id,
      trip_id,
      COUNT(*) AS num_AISdisabling
    FROM (
      SELECT
        vessel_id,
        event_start,
        event_end
      FROM
        `pipe_ais_v3_published.product_events_ais_disabling`) a
    INNER JOIN (
      SELECT
        vessel_id,
        trip_id,
        trip_start
      FROM
        raw_voyages)
    USING
      (vessel_id)
    WHERE
      event_start BETWEEN trip_start AND end_day()
    GROUP BY
      vessel_id, trip_id
      ),

  --------------------------------------
  -- label voyage if it had at least
  -- one encounter event
  --------------------------------------
  add_encounters AS (
    SELECT
      a.*,
      -- b.num_encounters,
      CASE WHEN b.num_encounters IS NULL THEN 0 ELSE b.num_encounters END AS num_encounters,
    FROM
      named_voyages AS a
    LEFT JOIN
      num_encounters b
    USING
      (vessel_id)
    GROUP BY
      1,2,3,4,5,6,7,8,9,10,11,12,13,14),

--------------------------------------
-- label voyage if it had at least
-- one loitering event
--------------------------------------
  add_loitering AS (
    SELECT
      c.*,
      -- d.num_loitering,
      CASE WHEN d.num_loitering IS NULL THEN 0 ELSE d.num_loitering END AS num_loitering,
    FROM
      add_encounters c
    LEFT JOIN
      num_loitering d
    USING
      (vessel_id)
    GROUP BY
      1,2,3,4,5,6,7,8,9,10,11,12,13,14,15),

--------------------------------------
-- label voyage if it had at least
-- one fishing event
--------------------------------------
  add_fishing AS (
    SELECT
      e.*,
      CASE WHEN f.num_fishing IS NULL THEN 0 ELSE f.num_fishing END AS num_fishing
    FROM
      add_loitering AS e
    LEFT JOIN
      num_fishing f
    USING
      (vessel_id)
    GROUP BY
      1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16),

--------------------------------------
-- label voyage if it had at least one gap event
--------------------------------------
  add_gaps AS (
    SELECT
      g.*,
      CASE WHEN h.num_AISdisabling IS NULL THEN 0 ELSE h.num_AISdisabling END AS num_AISdisabling
    FROM
      add_fishing AS g
    LEFT JOIN
      num_gaps h
    USING
      (vessel_id)
    GROUP BY
      1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17),

--------------------------------------
-- add trip duration
--------------------------------------
  voyage_duration AS (
    SELECT
      *,
      ROUND(TIMESTAMP_DIFF(end_day(), trip_start, HOUR)/24, 1) AS trip_duration_days,
    FROM
      add_gaps
        ),

----------------------------------------------------------
-- Join AIS coverage info to voyages and prep data fields for union to events
----------------------------------------------------------
voyages AS (
  SELECT
    *
  FROM voyage_duration
  LEFT JOIN (
    SELECT
      trip_id,
      percent_ais_voyage
    FROM `world-fishing-827.scratch_joef.PHL_aiscoverage_sep1-nov6`)
  USING (trip_id)
  ),

----------------------------------------------------------
-- Label ssvids with evidence of spoofing
----------------------------------------------------------
id_spoofers AS(
  SELECT ssvid
  FROM `world-fishing-827.pipe_ais_v3_published.vi_ssvid_byyear_v20240701`
  WHERE
      (year = 2023 OR year = 2024) AND
      # MMSI broadcast 2 or more names in overlapping segments for > 24 h, GFW criteria
      (activity.overlap_hours_multinames >= 24 OR
      # MMSI used by multiple vessels simultaneously for more than 3 days
      activity.overlap_hours >= 24*3 OR
      # MMSI offsetting position
      activity.offsetting IS TRUE)
  ),

----------------------------------------------------------
-- add spoofing indicator to master list (if ssvid spoofed in any of the 3 years of analysis)
----------------------------------------------------------
add_spoofer AS(
  SELECT
    *,
    IF (ssvid IN (
        SELECT ssvid FROM id_spoofers),
        TRUE, FALSE) possible_spoofing
  FROM voyages
),

add_active AS (
  SELECT
    trip_id,
    vessel_id,
    ssvid,
    trip_start AS trip_start_timestamp,
    start_iso3 AS start_port_iso3,
    start_label AS start_port_name,
    CASE WHEN vessel_id IN (select vessel_id from `world-fishing-827.scratch_joef.NPFC_vessels_sep01to08`)
        THEN TRUE ELSE FALSE END AS active_in_AOI,
    num_encounters,
    num_loitering,
    num_fishing,
    num_AISdisabling,
    percent_ais_voyage,
    possible_spoofing
  FROM add_spoofer),

--------------------------------------
-- flag vessels that visited ports of interest - either kuril islands or busan korea
--------------------------------------
kuril AS (
  SELECT *
  FROM(
    SELECT
      ssvid,
      vessel_id,
      timestamp,
      anchorage_id,
      end_timestamp
    FROM `world-fishing-827.pipe_ais_v3_published.port_visits`,
    UNNEST(events)
  )
  WHERE timestamp BETWEEN TIMESTAMP("2022-01-01") AND TIMESTAMP("2024-09-08")
  AND end_timestamp <TIMESTAMP("2024-09-08")
  AND
  anchorage_id IN(
      SELECT
        s2id
      FROM `world-fishing-827.anchorages.named_anchorages_v20240117`
      where label IN ("KURILSK", "RUS-291", "YUZHO KURILSK", "MALOKURILSK", "KRABOZAVODSK"))),

busan AS (
  SELECT *
  FROM(
    SELECT
      ssvid,
      vessel_id,
      timestamp,
      anchorage_id,
      end_timestamp
    FROM `world-fishing-827.pipe_ais_v3_published.port_visits`,
    UNNEST(events)
    --   vessel_id,
    --   event_start,
    --   JSON_EXTRACT_SCALAR(event_vessels, "$[0].ssvid") as ssvid,
    --   JSON_EXTRACT_SCALAR(event_info, "$.start_anchorage.anchorage_id") as start_anchorage,
    -- FROM `world-fishing-827.pipe_ais_v3_published.product_events_port_visit_v20240802`
  )
  WHERE timestamp BETWEEN TIMESTAMP("2022-01-01") AND TIMESTAMP("2024-09-08")
  AND end_timestamp <TIMESTAMP("2024-09-08")
  AND
  anchorage_id IN(
      SELECT
        s2id
      FROM `world-fishing-827.anchorages.named_anchorages_v20240117`
      where label IN ("BUSAN") AND iso3 = 'KOR')),

flag_port_use AS (
  SELECT
    *,
    CASE WHEN vessel_id IN (select vessel_id from kuril)
        THEN TRUE ELSE FALSE END AS visited_kurils,
    CASE WHEN vessel_id IN (select vessel_id from busan)
        THEN TRUE ELSE FALSE END AS visited_busan,
  FROM add_active
),

--------------------------------------
-- add rfmo auth info
--------------------------------------
prep_rfmo_auth AS(
    -- SECOND merge/concatenate all rfmos authorized for duration of voyage by trip_id
    SELECT
      trip_id,
      STRING_AGG(authorization_source, ', ') AS active_registry
    FROM (
      -- FIRST join each voyage to any rfmo it was authorized the entire time
      SELECT *
      FROM flag_port_use
      LEFT JOIN (
        SELECT
          ssvid,
          authorized_from,
          authorized_to,
          source_code AS authorization_source
        FROM `world-fishing-827.pipe_ais_v3_published.identity_authorization_v20240701`
        GROUP BY ssvid, authorized_from, authorized_to, authorization_source
      )
      USING (ssvid)
      where trip_start_timestamp > authorized_from AND trip_start_timestamp < authorized_to
    )
    GROUP BY trip_id
  )

-- join_rfmo_auth AS(
  SELECT
    *
  FROM flag_port_use
  LEFT JOIN prep_rfmo_auth
  USING (trip_id)



/*

      */
