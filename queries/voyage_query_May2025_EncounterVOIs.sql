--------------------------------------
  -- Query to pull voyages for 4 VOIs for May 2025 DFO NPG patrol


CREATE TEMP FUNCTION  voyage_start_date() AS (TIMESTAMP('2024-01-01 00:00:00 UTC'));

CREATE TEMP FUNCTION  start_year() AS (2025);
CREATE TEMP FUNCTION  end_year() AS (2025);

## set active period of interest
CREATE TEMP FUNCTION  active_period_start() AS (TIMESTAMP('2025-03-01 00:00:00 UTC'));
CREATE TEMP FUNCTION  active_period_end() AS (TIMESTAMP('2025-06-01 23:59:59 UTC'));

## set the current/ending day to calculate voyage duration up to (GFW) real time AND to ensure no voyage starts after it (also need for events bounding)..
CREATE TEMP FUNCTION  end_day() AS (TIMESTAMP('2025-05-29 23:59:59 UTC'));

CREATE or replace TABLE `world-fishing-827.scratch_joef.DFO_VOI_2025_voyages` AS

--------------------------------------
WITH

----------------------------------------------------------
-- Define list of VOIs
----------------------------------------------------------

----------------------------------------------------------
-- pull initial vessel info / format for rest of query - 2849 initially
----------------------------------------------------------
AOI_vessels AS(
 SELECT DISTINCT
   vessel_id,
   year,
   IFNULL(IFNULL(gfw_best_flag, core_flag), mmsi_flag) AS vessel_iso3,
   'DFO_VOI' AS origin_list,
   prod_geartype AS gear_type
  FROM
    `pipe_ais_v3_published.product_vessel_info_summary`
  WHERE
   year = 2024
   AND ssvid  IN ("273214530", "273619760", "273219690", "273290040")

  ),

----------------------------------------------------------
-- voyages for all identified vessels in time period
----------------------------------------------------------
  voyages AS (
    SELECT
      *
    FROM (
      SELECT
        ssvid,
        vessel_id,
        trip_start,
        trip_start_anchorage_id,
        trip_start_visit_id,
        -- trip_start_confidence,
        trip_end,
        trip_end_anchorage_id,
        trip_end_visit_id,
        -- trip_end_confidence,
        trip_id,
      FROM
        `pipe_ais_v3_published.voyages_c3`
      WHERE
        trip_start >= voyage_start_date() AND trip_start <= active_period_end()
        AND (trip_end >= active_period_start() OR trip_end IS NULL)
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

--------------------------------------
-- Identify how many encounters occurred on each voyage
--------------------------------------
-- pipe 3
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
      start_distance_from_shore_km
    FROM `pipe_ais_v3_published.product_events_encounter`) enc
    INNER JOIN (
      SELECT
        vessel_id,
        trip_id,
        trip_start
      FROM
        named_voyages) voyages
    USING (vessel_id)
      WHERE event_start BETWEEN trip_start AND end_day()
      AND  product_shiptype != 'gear' AND enc_product_shiptype != 'gear'
    GROUP BY
       vessel_id, trip_id
    ),

--------------------------------------
-- Identify how many loitering events occurred on each voyage
--------------------------------------
  -- pipe3
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
        named_voyages) b
    USING
      (vessel_id)
    WHERE event_start BETWEEN trip_start AND end_day()
    GROUP BY
      vessel_id, trip_id
      ),

--------------------------------------
-- Identify how many fishing events occurred on each voyage
--------------------------------------
-- pipe3
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
        named_voyages)
    USING
      (vessel_id)
    WHERE event_start BETWEEN trip_start AND end_day()
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
      b.num_encounters,
    IF
      (b.num_encounters > 0, TRUE, FALSE) AS had_encounter
    FROM
      named_voyages AS a
    LEFT JOIN
      num_encounters b
    USING
      (vessel_id,
        trip_id)
    GROUP BY
      1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18),

--------------------------------------
-- label voyage if it had at least
-- one loitering event
--------------------------------------
  add_loitering AS (
    SELECT
      c.*,
      d.num_loitering,
    IF
      (d.num_loitering > 0, TRUE, FALSE) AS had_loitering
    FROM
      add_encounters c
    LEFT JOIN
      num_loitering d
    USING
      (vessel_id,
        trip_id)
    GROUP BY
      1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20),

--------------------------------------
-- label voyage if it had at least
-- one fishing event
--------------------------------------
  add_fishing AS (
    SELECT
      e.*,
      f.num_fishing,
    IF
      (f.num_fishing > 0, TRUE, FALSE) AS had_fishing
    FROM
      add_loitering AS e
    LEFT JOIN
      num_fishing f
    USING
      (vessel_id,
        trip_id)
    GROUP BY
      1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22),

--------------------------------------
-- Identify vessel ids with less than
-- one position per two days and
-- no identity information.
--
-- Justification: there are some
-- vessels that have vessel_ids with
-- no identity information, but which
-- represent a quality track.
--------------------------------------
  poor_vessel_ids AS (
    SELECT
      *,
    IF
      (SAFE_DIVIDE(pos_count,TIMESTAMP_DIFF(last_timestamp, first_timestamp, DAY)) < 0.5
        AND (shipname.value IS NULL
          AND callsign.value IS NULL
          AND imo.value IS NULL), TRUE, FALSE) AS poor_id
    FROM
      `pipe_ais_v3_published.vessel_info` ),

--------------------------------------
-- add trip duration and label 'poor id' vessels
--------------------------------------
  vessel_voyages AS (
    SELECT
      *,
      ROUND(TIMESTAMP_DIFF(end_day(), trip_start, HOUR)/24, 2) AS trip_duration_days,
      CASE WHEN vessel_id IN(SELECT vessel_id FROM poor_vessel_ids WHERE poor_id IS TRUE) THEN TRUE ELSE FALSE END AS poor_id
    FROM
      add_fishing
        ),

--------------------------------------
-- all qualifying voyages
--------------------------------------
  all_voyages AS (
    SELECT
      2025 AS year,
      vessel_id,
      ssvid,
      vessel_iso3,
      origin_list,
      gear_type,
      trip_id,
      trip_start,
      start_iso3 AS start_port_iso3,
      start_label AS start_port_label,
      trip_end,
      end_iso3 AS end_port_iso3,
      end_label AS end_port_label,
      -- trip_start_confidence,
      trip_duration_days,
      poor_id,
      num_encounters,
      num_loitering,
      num_fishing
    FROM
      vessel_voyages
        ),

----------------------------------------------------------
-- high/med/low confidence fishing vessels
----------------------------------------------------------

-- HIGH: MMSI in the fishing_vessels_ssvid table.
-- These are vessels on our best fishing list and that have reliable AIS data

  high_conf_fishing AS (
     SELECT DISTINCT
       ssvid,
       year,
      IFNULL(IFNULL(gfw_best_flag, core_flag), mmsi_flag) AS vessel_iso3,
       '1' as class_confidence,
       'fishing' AS vessel_class,
       prod_geartype AS gear_type
     FROM
      `pipe_ais_v3_published.product_vessel_info_summary`
     WHERE
      year >= start_year() AND year <= end_year()
      AND prod_shiptype IN ("fishing")
      AND on_fishing_list_best
     ),

-- MED: All MMSI on our best fishing list not included in the high category.
-- These are likely fishing vessels that primarily get excluded due to data issues
-- (e.g. spoofing, offsetting, low activity)

  med_conf_fishing AS (
    SELECT DISTINCT
      ssvid,
      year,
      IFNULL(IFNULL(gfw_best_flag, core_flag), mmsi_flag) AS vessel_iso3,
      '2' as class_confidence,
      'fishing' AS vessel_class,
      prod_geartype AS gear_type
    FROM
      `pipe_ais_v3_published.product_vessel_info_summary` AS vi_table
    WHERE
    year >= start_year() AND year <= end_year()
    AND prod_shiptype IN ("fishing")
    AND on_fishing_list_sr
    AND
    -- anti join to get vessels that don't match high conf list by ssvid or year:
    NOT EXISTS (
      SELECT ssvid, year
      FROM high_conf_fishing
      WHERE vi_table.ssvid = high_conf_fishing.ssvid AND vi_table.year = high_conf_fishing.year
    )
    ),

-- LOW: MMSI that are on one of our three source fishing lists (registry, neural net, self-reported)
-- but not included in either the med or high list. These are MMSI for which we have minimal
-- or conflicting evidence that they are a fishing vessel.

  low_conf_fishing AS (
    SELECT DISTINCT
      ssvid,
      year,
      IFNULL(IFNULL(gfw_best_flag, core_flag), mmsi_flag) AS vessel_iso3,
      '3' as class_confidence,
      'fishing' AS vessel_class,
      prod_geartype AS gear_type
    FROM
      `pipe_ais_v3_published.product_vessel_info_summary` AS vi_table
    WHERE (
      prod_shiptype = 'fishing'
      OR prod_shiptype = 'discrepancy'
      OR potential_fishing
      OR on_fishing_list_sr
      )
    AND year >= start_year() AND year <= end_year()
    -- anti joins to get vessels that don't match high/med conf list by ssvid or year:
    AND NOT EXISTS (
      SELECT ssvid, year
      FROM high_conf_fishing
      WHERE vi_table.ssvid = high_conf_fishing.ssvid AND vi_table.year = high_conf_fishing.year
    )
    AND NOT EXISTS (
      SELECT ssvid, year
      FROM med_conf_fishing
      WHERE vi_table.ssvid = med_conf_fishing.ssvid AND vi_table.year = med_conf_fishing.year
    )
    ),

---------------------------------------------------------------
-- List of carriers according to vessel registries - high confidence
---------------------------------------------------------------
  reg_carriers AS (
    SELECT DISTINCT -- generics for flag and gear as there are duplicates in v database which will duplicate voyages
      ssvid,
      'flag' AS vessel_iso3,
      '1' AS class_confidence,
      'carrier' AS vessel_class,
      'geartype' AS gear_type,
      -- first_timestamp,
      -- last_timestamp
    FROM
      `pipe_ais_v3_published.identity_core`
    WHERE
      TIMESTAMP(first_timestamp) <= end_day() AND
      TIMESTAMP(last_timestamp) >= active_period_start() AND
      is_carrier = TRUE AND
      geartype IN ("reefer","specialized_reefer") AND
      n_shipname IS NOT NULL AND
      flag IS NOT NULL
  ),

---------------------------------------------------------------
-- nn carriers capable of transshipping at sea - not included in is_carrier
---------------------------------------------------------------
  nn_carriers_med_confidence AS (
    SELECT DISTINCT
      ssvid,
      best.best_flag AS vessel_iso3,
      '2' AS class_confidence,
      'carrier' AS vessel_class,
      best.best_vessel_class AS gear_type,
      -- activity.first_timestamp,
      -- activity.last_timestamp
    FROM
      `pipe_ais_v3_published.vi_ssvid_byyear_v`
    WHERE
      TIMESTAMP(activity.first_timestamp) <= end_day() AND
      TIMESTAMP(activity.last_timestamp) >= active_period_start() AND
      best.best_vessel_class IN ("specialized_reefer", "reefer") AND
      ssvid NOT IN(SELECT ssvid FROM reg_carriers)
  ),

---------------------------------------------------------------
-- other carriers not included in is_carrier
---------------------------------------------------------------
  nn_carriers_low_confidence AS (
    SELECT DISTINCT
      ssvid,
      best.best_flag AS vessel_iso3,
      '3' AS class_confidence,
      'carrier' AS vessel_class,
      best.best_vessel_class AS gear_type,
      activity.first_timestamp,
      activity.last_timestamp
    FROM
      `pipe_ais_v3_published.vi_ssvid_byyear_v`
    WHERE
      TIMESTAMP(activity.first_timestamp) <= end_day() AND
      TIMESTAMP(activity.last_timestamp) >= active_period_start() AND
      best.best_vessel_class IN ("container_reefer", "cargo_or_reefer") AND
      ssvid NOT IN (SELECT ssvid FROM reg_carriers) AND
      ssvid NOT IN (SELECT ssvid FROM nn_carriers_med_confidence)
  ),

----------------------------------------------------------
-- combined carrier table info
----------------------------------------------------------
  fishing_and_carrier_vessels AS (
    SELECT
      ssvid, class_confidence, vessel_class
    FROM
      high_conf_fishing
    UNION ALL

    SELECT
      ssvid, class_confidence, vessel_class
    FROM
      med_conf_fishing
    UNION ALL

    SELECT
      ssvid, class_confidence, vessel_class
    FROM
      low_conf_fishing
    UNION ALL

    SELECT
      ssvid, class_confidence, vessel_class
    FROM
      reg_carriers
    UNION ALL

    SELECT
      ssvid, class_confidence, vessel_class
    FROM
      nn_carriers_med_confidence
    UNION ALL

    SELECT
      ssvid, class_confidence, vessel_class
    FROM
      nn_carriers_low_confidence
  ),

--------------------------------------
-- Remove any duplicates between categories (fishing, carrier) by
-- selecting duplicate vessel with the higher confidence level
-- note if vessels have the same conf level, (arbitrarily) selecting fishing, then carrier...
--------------------------------------
  simplify_vessels AS(
    SELECT DISTINCT
      ssvid,
      FIRST_VALUE (class_confidence) OVER (
        PARTITION BY ssvid
        ORDER BY class_confidence ASC, vessel_class DESC) AS class_confidence,
      FIRST_VALUE (vessel_class) OVER (
        PARTITION BY ssvid
        ORDER BY class_confidence ASC, vessel_class DESC) AS initial_vessel_class,
  FROM fishing_and_carrier_vessels
  ),

--------------------------------------
-- join class_confidence/class and filter by carrier/fisher
--------------------------------------
  select_fv_cv AS (
    SELECT
      *
    FROM all_voyages
    LEFT JOIN simplify_vessels
    USING (ssvid)
  ),

--------------------------------------
-- add vessel info from all_vessels table
--------------------------------------
  add_vessel_info AS (
    SELECT * FROM(
    SELECT
      vessel_id,
      year,
      origin_list,
      initial_vessel_class,
      class_confidence,
      trip_id,
      trip_start,
      start_port_iso3,
      start_port_label,
      trip_end,
      end_port_iso3,
      end_port_label,
      -- trip_start_confidence,
      trip_duration_days,
      poor_id,
      num_encounters,
      num_loitering,
      num_fishing
    FROM select_fv_cv )
    JOIN (
      SELECT
        vessel_id,
        ssvid,
        year,
        shipname,
        callsign,
        imo,
        gfw_best_flag AS vessel_flag_best,
        prod_shiptype AS vessel_class_best,
        prod_geartype AS geartype_best
      FROM
        `pipe_ais_v3_published.product_vessel_info_summary`)
      USING
        (vessel_id, year)),

--------------------------------------
-- organize table

-- this comment from round 1 - there are instances of multiple ssvids per vessel_id, but trips are different (not duplicated).
-- possible that spoofing or that the return visit wasn't recorded
--------------------------------------
  clean_info AS(
    SELECT
      -- CASE WHEN ssvid IN ("273214530", "273619760", "273219690", "273290040")
      --   THEN TRUE ELSE FALSE END AS DFO_VOI,
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
      trip_id,
      trip_start,
      start_port_iso3,
      start_port_label,
      trip_end,
      end_port_iso3,
      end_port_label,
      trip_duration_days,
      CASE WHEN num_encounters IS NULL THEN 0 ELSE num_encounters END AS num_encounters,
      CASE WHEN num_loitering IS NULL THEN 0 ELSE num_loitering END AS num_loitering,
      CASE WHEN num_fishing IS NULL THEN 0 ELSE num_fishing END AS num_fishing
    FROM add_vessel_info)

  SELECT
  *
  FROM
  clean_info
  -- WHERE ( class_confidence IS NOT NULL -- pick those with either 1, 2, or 3 for fv or cv
  --         OR DFO_VOI IS TRUE)
    ORDER BY ssvid
     /*
*/
