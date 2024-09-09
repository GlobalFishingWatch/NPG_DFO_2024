--------------------------------------
  -- From initial vessel list, pull summary voyage/AIS data for each vessel's current voyage

## set time frame of interest for voyages (i.e., voyages must start after this date to be included)
CREATE TEMP FUNCTION  voyage_start_date() AS (TIMESTAMP('2023-01-01 00:00:00 UTC'));

CREATE TEMP FUNCTION  start_year() AS (2024);
CREATE TEMP FUNCTION  end_year() AS (2024);

## set active period of interest
-- CREATE TEMP FUNCTION  event_period_start() AS (TIMESTAMP('2024-07-01 00:00:00 UTC'));
-- CREATE TEMP FUNCTION  event_period_end() AS (TIMESTAMP('2024-09-06 23:59:59 UTC'));

## set the current/ending day to calculate voyage duration up to (GFW) real time AND to ensure no voyage starts after it (also need for events bounding)..
CREATE TEMP FUNCTION  end_day() AS (TIMESTAMP('2024-09-08 23:59:59 UTC'));

-- CREATE or replace TABLE `world-fishing-827.scratch_joef.NPG_voyages_09082024` AS

WITH

----------------------------------------------------------
-- pull vessel_ids / info from initially pulled vessels
----------------------------------------------------------
vessel_list AS (
  SELECT
    DFO_VOI,
    vessel_id,
    ssvid,
    imo,
    shipname,
    callsign,
    vessel_flag_best,
    vessel_class_best,
    class_confidence,
    poor_id,
    geartype_best,
  FROM `world-fishing-827.scratch_joef.NPFC_vessels_jul22-aug26`
  ),

----------------------------------------------------------
-- voyages for all identified fishing vessels
----------------------------------------------------------
  raw_voyages AS (
    SELECT
        vessels.*,
        -- voyages.ssvid,
        -- voyages.vessel_id,
        voyages.trip_start,
        -- voyages.trip_end,
        voyages.trip_start_anchorage_id,
        -- voyages.trip_end_anchorage_id,
        voyages.trip_start_visit_id,
        -- voyages.trip_end_visit_id,
        voyages.trip_id,
    FROM (
      SELECT
        *
      FROM
      `pipe_ais_v3_published.voyages_c3`
      WHERE
        (trip_start >= voyage_start_date() OR trip_start IS NULL) AND
        (trip_end IS NULL) AND
        vessel_id IN (select vessel_id from `world-fishing-827.scratch_joef.NPFC_vessels_jul22-aug26`)
        ) AS voyages
    INNER JOIN vessel_list AS vessels
    USING (vessel_id)
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
-- Add names to voyages (start)
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
      1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18),

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
      1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19),

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
      1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20),

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
      1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21),

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
    FROM `world-fishing-827.scratch_joef.NPG_aiscoverage_1jul_to_6sep`)
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
    DFO_VOI,
    vessel_id,
    ssvid,
    imo,
    shipname,
    callsign,
    vessel_flag_best,
    vessel_class_best,
    class_confidence,
    poor_id,
    geartype_best,

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
