--------------------------------------
  -- Query to identify fishing and carrier vessels in the NPG AOI.
  -- using geojson of npfc AOI to pull active vessels

  -- this version also fixes gear in encounters vs the old version

## set time frame of interest for voyages (i.e., voyages must start after this date to be included)
CREATE TEMP FUNCTION  voyage_start_date() AS (TIMESTAMP('2023-01-01 00:00:00 UTC'));

## set start date for identity core table. active records/ last timestamp will be 1+ months earlier than present
CREATE TEMP FUNCTION  registry_start_date() AS (TIMESTAMP('2024-05-01 00:00:00 UTC'));

CREATE TEMP FUNCTION  start_year() AS (2024);
CREATE TEMP FUNCTION  end_year() AS (2024);

## set active period of interest
CREATE TEMP FUNCTION  active_period_start() AS (TIMESTAMP('2024-09-02 00:00:00 UTC'));
CREATE TEMP FUNCTION  active_period_end() AS (TIMESTAMP('2024-09-08 23:59:59 UTC'));

## set the current/ending day to calculate voyage duration up to (GFW) real time AND to ensure no voyage starts after it (also need for events bounding)..
CREATE TEMP FUNCTION  end_day() AS (TIMESTAMP('2024-08-24 23:59:59 UTC'));

CREATE TABLE `world-fishing-827.scratch_joef.NPFC_vessels_sep01to08` AS

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
  ST_GEOGFROMGEOJSON('{ "type": "Polygon", "coordinates":[[[143.2287615352053,32.99819736464531],[143.2397340334017,32.90343206476063],[143.27264115325275,32.81266801082668],[143.3261755654543,32.72932432472997],[143.3982717690773,32.65653117366074],[143.4861889997583,32.5970162798739],[143.58661536157985,32.55300690069037],[143.69578930778349,32.52615018405615],[143.8096345396461,32.517454214508305],[143.92390437425684,32.52725148083002],[179.78574281971274,32.458518824609804],[179.78580370272658,32.45878110339761],[179.7910192634151,32.457464110099956],[179.9,32.448821830877996],[179.9,32.45078832276703],[196.94202675879419,32.520039335949996],[197.05126539905606,32.52011678056697],[197.1581356610894,32.53692817385178],[197.25901325306853,32.56989727600613],[197.35047075142205,32.61790127824845],[197.42938840625658,32.67930749969146],[197.49305660292922,32.75202643093447],[197.5392671594366,32.83357972185416],[197.56639065501557,32.92118131176615],[197.5734370400717,33.011829495741786],[197.78966202912383,50.95108174273079],[197.78135410618486,51.04790503966885],[197.74221077341997,51.14161625296772],[197.6735953035039,51.228518087305865],[197.57807008005676,51.3051596798851],[197.45931751208465,51.36847807595436],[197.32200712678258,51.41592695897632],[197.171611847171,51.44558624213745],[197.01418045221178,51.456246648370204],[196.8560770720086,51.447464426641424],[181.52984393725964,48.51336319681802],[179.9,48.42919547043571],[174.6363999143698,49.537144944058824],[174.4600509238227,49.55447452796297],[172.8246697291087,49.578291411574526],[171.0456784567161,49.98478321366948],[169.76476164152433,50.6743077204898],[168.41127080734512,51.541650059893186],[168.2901122686193,51.60360732124255],[168.15170336035715,51.64953896364186],[168.00144184433358,51.67764313306818],[165.0557900402608,51.99980247807754],[164.90994866947864,52.005635021510486],[164.76493689587426,51.994978194443405],[164.6256450374082,51.96819321289612],[164.4967522347319,51.92618342440959],[164.38255702690373,51.870361083869646],[154.67815773346334,45.01018742460521],[145.9643766126254,39.99760566706817],[145.86365873480872,39.91490831447921],[145.7902973410319,39.81728673090367],[145.7479870001575,39.70984535329931],[145.4227461233609,38.31671629347897],[144.45454058446725,36.48856263064692],[144.41039372098257,36.34880316672094],[144.23205652677981,34.94153632192215],[144.0315894696195,34.51680161223413],[143.33072477851175,33.65695565428085],[143.27510788859828,33.570954389365745],[143.24088959923552,33.47805025924585],[143.22931973813058,33.381860007144596],[143.2287615352053,32.99819736464531]]] } ')
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
    AND clean_segs IS TRUE
  # Filter to polygon using ST_CONTAINS.
  # You need to create a geometry object for each position using ST_GEOPOINT
  AND ST_CONTAINS((SELECT NPFC_geojson FROM AOI), ST_GEOGPOINT(lon, lat))
  ),

----------------------------------------------------------
-- pull initial vessel info / format for rest of query - 2849 initially
----------------------------------------------------------
AOI_vessels AS(
 SELECT DISTINCT
   vessel_id,
   ssvid,
   year,
   IFNULL(IFNULL(gfw_best_flag, core_flag), mmsi_flag) AS vessel_iso3,
   'inAOI' AS origin_list,
   prod_geartype AS gear_type
  FROM
    `pipe_ais_v3_published.product_vessel_info_summary_v20240701`
  WHERE
   year = 2024
   AND vessel_id IN (SELECT vessel_id FROM active_vessel_ids)
  ),

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
  add_poor_vessels AS (
    SELECT
      *,
      CASE WHEN vessel_id IN(SELECT vessel_id FROM poor_vessel_ids WHERE poor_id IS TRUE) THEN TRUE ELSE FALSE END AS poor_id
    FROM
      AOI_vessels
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
      `pipe_ais_v3_published.product_vessel_info_summary_v20240701`
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
      `pipe_ais_v3_published.product_vessel_info_summary_v20240701` AS vi_table
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
      `pipe_ais_v3_published.product_vessel_info_summary_v20240701` AS vi_table
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
      `pipe_ais_v3_published.identity_core_v20240701`
    WHERE
      TIMESTAMP(first_timestamp) <= end_day() AND
      TIMESTAMP(last_timestamp) >= registry_start_date() AND
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
      `pipe_ais_v3_published.vi_ssvid_byyear_v20240701`
    WHERE
      TIMESTAMP(activity.first_timestamp) <= end_day() AND
      TIMESTAMP(activity.last_timestamp) >= registry_start_date() AND
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
      `pipe_ais_v3_published.vi_ssvid_byyear_v20240701`
    WHERE
      TIMESTAMP(activity.first_timestamp) <= end_day() AND
      TIMESTAMP(activity.last_timestamp) >= registry_start_date() AND
      best.best_vessel_class IN ("container_reefer", "cargo_or_reefer") AND
      ssvid NOT IN (SELECT ssvid FROM reg_carriers) AND
      ssvid NOT IN (SELECT ssvid FROM nn_carriers_med_confidence)
  ),

----------------------------------------------------------
-- combined table info
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
    FROM add_poor_vessels
    LEFT JOIN simplify_vessels
    USING (ssvid)
  ),

--------------------------------------
-- add vessel info from all_vessels table - 1394
--------------------------------------
  add_vessel_info AS (
    SELECT * FROM(
    SELECT
      vessel_id,
      year,
      origin_list,
      initial_vessel_class,
      class_confidence,
      poor_id,
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
        `pipe_ais_v3_published.product_vessel_info_summary_v20240501`)
      USING
        (vessel_id, year)),

--------------------------------------
-- organize table

-- this comment from round 1 - there are instances of multiple ssvids per vessel_id, but trips are different (not duplicated).
-- possible that spoofing or that the return visit wasn't recorded
--------------------------------------
  clean_info AS(
    SELECT
      CASE WHEN ssvid IN ("412549267","352182000","412420954","412420956","412421009","412421008","412421012","412421011","412209139","412209142","412421035","412421036","412421076","412209136","412421075","413205170","413205180","412420952","412421024","412549556","412200083","416037500","273354460","273335230","416240500","416248800","416046500","576990000","577079000","416002174","416002998","416000369","416002147","416000275","416002149","416002231","416002222","416002201","416000206","416001054","416000833","416217800","416227600","416002445","416000993","416000855","416004329","416000141","416000411","416225800","416002096","416001233","416002826","416002643","416002724","416002194","416000013","416000139","416001799","412331087","412329655","412329656","412329657","412329658","412331054","412331055","412331088","576770000","431601150","431001079","431501883","431200070","412450001","412549375","412677240","412677270","412440029","412440031","412440032","412440548","412440549","412440691","412440692","412440693","412440694","412440695","412549363","412549364","412549365","412549366","412549367","412549368","412549369","412549371","412549372","412549373","412549374","412549386","412549387","412549388","412549389","412549391","412549392","412549393","412549394","412440697","412440698","412440792","412440793","412440794","412440795","412677070","412440004","412440834","412440723","412440493","457178000","412671880","412401260","412420829","412420829","412420209","412420211","412672220","412672210","412672170","412672160","412672130","412672120","412672040","412672030","412672020","412671990","412671980","412420908","412420912","412671930","412671920","412671910","412671890","412401250","412671870","412420872","412420873","412420978","412420982","412421044","412421043","412421042","412421039","412421139","412421146","412421038","412421037","412549082","412549083","412549084","412549085","412549086","412690730","412690720")
      OR
      imo IN ("9974735","9677595","8786777","8786789","8786480","8786492","8786507","8786519","8540238","8540252","8786806","8786820","8788062","8567303","8540525","8788074","8537360","8549636","8786791","8786521","8607244","8724339","8813582","8655291","8703529","8747977","8676635","9766724","8655605","9694220","9688740","8540991","8539930","8550386","8539849","8540111","8540472","8539887","8534423","8551249","8530972","8792714","8793524","8554370","8530984","8534473","8554356","8554394","8794059","8550879","8550946","8542690","8549832","8342179","8792752","8342155","8542781","8543101","8550702","8550934","8550867","8685478","9717448","9717450","9717462","9717474","9752929","9752931","9769556","8996114","8344567","8344206","8344218","8343721","X3754182","9934589","7815246","8403698","8820509","9031947","9016571","9828704","9828716","9872561","9872573","9888273","9888285","9888297","9934503","9933717","9933729","9933731","9934515","9934527","9934539","9934541","9934553","9934565","9934577","9940497","9940538","9940540","9940552","9940576","9940590","9940617","9940629","9870111","9870587","9870599","9916692","9916654","9916707","9916721","9096507","8414295","9920954","9897066","8790596","8994013","9204087","8783373","8783385","8783426","9160097","8708294","8911047","8783127","8783139","8783141","8783153","8783165","8783177","8783189","8783191","8783232","8783244","8783256","8783268","8783270","8783282","8783294","8783309","8783311","8775170","8775182","8783323","8783335","8783347","8783359","8783361","8783397","8775156","8775168","8783402","8783414","9819569","9819571","9819583","9819595","9861110","9861122","9819600","9819612","9888247","9888417","9888259","9888261","9888431","8774877","8774865")
      THEN TRUE ELSE FALSE END AS DFO_VOI,

      CASE WHEN ssvid IN (select ssvid from `world-fishing-827.scratch_joef.NPFC_vessels_jul22-aug26`)
      THEN TRUE ELSE FALSE END AS Round1_review,
      vessel_id,
      ssvid,
      imo,
      shipname,
      callsign,
      vessel_flag_best,
      vessel_class_best,
      class_confidence,
      poor_id,
      geartype_best
    FROM add_vessel_info)

  SELECT
  *
  FROM
  clean_info
  WHERE ( class_confidence IS NOT NULL -- pick those with either 1, 2, or 3 for fv or cv
          OR DFO_VOI IS TRUE) -- AND Round1_review IS FALSE
          AND vessel_class_best NOT IN ("gear")
    ORDER BY ssvid
     /*
*/
