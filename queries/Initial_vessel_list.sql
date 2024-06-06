--------------------------------------
  -- Query to identify fishing and carrier vessels in the NPG AOI.
  -- note initial mmsi list drawn from GFW map in the AOI

## set time frame of interest
CREATE TEMP FUNCTION  start_date() AS (TIMESTAMP('2023-01-01 00:00:00 UTC'));
CREATE TEMP FUNCTION  start_year() AS (2023);
## voyages will be truncated to this end timestamp, if needed
CREATE TEMP FUNCTION  end_date() AS (TIMESTAMP('2024-06-10 23:59:59 UTC'));
CREATE TEMP FUNCTION  end_year() AS (2024);

## set the current day to calculate voyage duration up to (GFW) real time later in query
CREATE TEMP FUNCTION  current_day() AS (TIMESTAMP('2024-06-06 23:59:59 UTC'));

--------------------------------------
WITH

----------------------------------------------------------
-- Define list of VOIs
----------------------------------------------------------

----------------------------------------------------------
-- vessels pulled from GFW map active in AOI during period of interest (csv uploaded to BQ)
----------------------------------------------------------
 AOI_vessels AS(
   SELECT DISTINCT
    --  ssvid,
     vessel_id,
     year,
     IFNULL(IFNULL(gfw_best_flag, core_flag), mmsi_flag) AS vessel_iso3,
    --  '2' as class_confidence,
     'inAOI' AS vessel_class,
     prod_geartype AS gear_type
    FROM
      `pipe_ais_v3_published.product_vessel_info_summary_v20240501`
    WHERE
     year = 2024
     AND ssvid IN (SELECT CAST(mmsi AS STRING) AS ssvid from `scratch_joef.NPG_vessel_presence_Jun1-Jun6`)
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
        (trip_start >= start_date() OR trip_start IS NULL) AND trip_end IS NULL
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
  SELECT
    vessel_id,
    trip_id,
    COUNT(*) AS num_encounters
  FROM (
    SELECT
      vessel_id,
      event_start,
      event_end
    FROM `pipe_ais_v3_published.product_events_encounter`) enc
    INNER JOIN (
      SELECT
        vessel_id,
        trip_id,
        trip_start
      FROM
        updated_pan_voyages) voyages
    USING (vessel_id)
      WHERE event_start >= trip_start
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
        trip_start,
        pan_crossing
      FROM
        updated_pan_voyages) b
    USING
      (vessel_id)
    WHERE event_start >= trip_start
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
        updated_pan_voyages)
    USING
      (vessel_id)
    WHERE event_start >= trip_start
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
      updated_pan_voyages AS a
    LEFT JOIN
      num_encounters b
    USING
      (vessel_id,
        trip_id)
    GROUP BY
      1,2,3,4,5,6,7,8,9,10,11,12,13,14,15),

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
      1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17),

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
      1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19),

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
      ROUND(TIMESTAMP_DIFF(current_day(), trip_start, HOUR)/24, 2) AS trip_duration_days,
      CASE WHEN vessel_id IN(SELECT vessel_id FROM poor_vessel_ids WHERE poor_id IS TRUE) THEN TRUE ELSE FALSE END AS poor_id
    FROM
      add_fishing
        ),

--------------------------------------
-- all qualifying voyages
--------------------------------------
  all_voyages AS (
    SELECT
      2024 AS year,
      ssvid,
      vessel_id,
      vessel_iso3,
      vessel_class,
      gear_type,
      trip_id,
      trip_start,
      start_iso3 AS start_port_iso3,
      start_label AS start_port_label,
      trip_start_confidence,
      trip_duration_days,
      poor_id,
      num_encounters,
      num_loitering,
      num_fishing
    FROM
      vessel_voyages
        ),

--------------------------------------
-- add vessel info from all_vessels table

-- note, depending on specific query, there may be
-- additional vessels dropped here that are not
-- present in all_vessels_byyear (compare to previous subquery)
--------------------------------------
  add_vessel_info AS (
    SELECT * FROM(
    SELECT
      vessel_id,
      year,
      vessel_class AS origin_list,
      trip_id,
      trip_start,
      start_port_iso3,
      start_port_label,
      trip_start_confidence,
      trip_duration_days,
      poor_id,
      num_encounters,
      num_loitering,
      num_fishing
    FROM all_voyages )
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
        `pipe_ais_v3_published.product_vessel_info_summary_v20240501`)
      USING
        (vessel_id, year)),

--------------------------------------
-- organize table, there are instances of multiple ssvids per vessel_id, but trips are different (not duplicated).
-- possible that spoofing or that the return visit wasn't recorded
--------------------------------------
  clean_info AS(
    SELECT
      CASE WHEN ssvid IN ("412549267","352182000","412420954","412420956","412421009","412421008","412421012","412421011","412209139","412209142","412421035","412421036","412421076","412209136","412421075","413205170","413205180","412420952","412421024","412549556","412200083","416037500","273354460","273335230","416240500","416248800","416046500","576990000","577079000","416002174","416002998","416000369","416002147","416000275","416002149","416002231","416002222","416002201","416000206","416001054","416000833","416217800","416227600","416002445","416000993","416000855","416004329","416000141","416000411","416225800","416002096","416001233","416002826","416002643","416002724","416002194","416000013","416000139","416001799","412331087","412329655","412329656","412329657","412329658","412331054","412331055","412331088","576770000","431601150","431001079","431501883","431200070","412450001","412549375","412677240","412677270","412440029","412440031","412440032","412440548","412440549","412440691","412440692","412440693","412440694","412440695","412549363","412549364","412549365","412549366","412549367","412549368","412549369","412549371","412549372","412549373","412549374","412549386","412549387","412549388","412549389","412549391","412549392","412549393","412549394","412440697","412440698","412440792","412440793","412440794","412440795","412677070","412440004","412440834","412440723","412440493","457178000","412671880","412401260","412420829","412420829","412420209","412420211","412672220","412672210","412672170","412672160","412672130","412672120","412672040","412672030","412672020","412671990","412671980","412420908","412420912","412671930","412671920","412671910","412671890","412401250","412671870","412420872","412420873","412420978","412420982","412421044","412421043","412421042","412421039","412421139","412421146","412421038","412421037","412549082","412549083","412549084","412549085","412549086","412690730","412690720")
      OR
      imo IN ("9974735","9677595","8786777","8786789","8786480","8786492","8786507","8786519","8540238","8540252","8786806","8786820","8788062","8567303","8540525","8788074","8537360","8549636","8786791","8786521","8607244","8724339","8813582","8655291","8703529","8747977","8676635","9766724","8655605","9694220","9688740","8540991","8539930","8550386","8539849","8540111","8540472","8539887","8534423","8551249","8530972","8792714","8793524","8554370","8530984","8534473","8554356","8554394","8794059","8550879","8550946","8542690","8549832","8342179","8792752","8342155","8542781","8543101","8550702","8550934","8550867","8685478","9717448","9717450","9717462","9717474","9752929","9752931","9769556","8996114","8344567","8344206","8344218","8343721","X3754182","9934589","7815246","8403698","8820509","9031947","9016571","9828704","9828716","9872561","9872573","9888273","9888285","9888297","9934503","9933717","9933729","9933731","9934515","9934527","9934539","9934541","9934553","9934565","9934577","9940497","9940538","9940540","9940552","9940576","9940590","9940617","9940629","9870111","9870587","9870599","9916692","9916654","9916707","9916721","9096507","8414295","9920954","9897066","8790596","8994013","9204087","8783373","8783385","8783426","9160097","8708294","8911047","8783127","8783139","8783141","8783153","8783165","8783177","8783189","8783191","8783232","8783244","8783256","8783268","8783270","8783282","8783294","8783309","8783311","8775170","8775182","8783323","8783335","8783347","8783359","8783361","8783397","8775156","8775168","8783402","8783414","9819569","9819571","9819583","9819595","9861110","9861122","9819600","9819612","9888247","9888417","9888259","9888261","9888431","8774877","8774865")
      THEN TRUE ELSE FALSE END AS DFO_VOI,
      ssvid,
      imo,
      poor_id,
      vessel_id,
      shipname,
      callsign,
      vessel_flag_best,
      vessel_class_best,
      geartype_best,
      trip_id,
      trip_start,
      start_port_iso3,
      start_port_label,
      trip_start_confidence,
      trip_duration_days,
      -- poor_id,
      CASE WHEN num_encounters IS NULL THEN 0 ELSE num_encounters END AS num_encounters,
      CASE WHEN num_loitering IS NULL THEN 0 ELSE num_loitering END AS num_loitering,
      CASE WHEN num_fishing IS NULL THEN 0 ELSE num_fishing END AS num_fishing
    FROM add_vessel_info)

  SELECT
  *
  FROM
  clean_info
  WHERE (vessel_class_best IN ("fishing", "carrier") OR DFO_VOI IS TRUE)
      AND
      (poor_id IS false OR DFO_VOI IS TRUE)
    ORDER BY ssvid
     /*
