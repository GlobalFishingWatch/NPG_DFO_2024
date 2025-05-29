--------------------------------------
  -- Query to identify fishing and carrier vessels in the NPG AOI.
  -- using geojson of npfc AOI to pull active vessels

  -- this version also fixes gear in encounters vs the old version

## set time frame of interest for voyages (i.e., voyages must start after this date to be included)
CREATE TEMP FUNCTION  voyage_start_date() AS (TIMESTAMP({start_date}));
CREATE TEMP FUNCTION  year() AS ({year});

## set active period of interest
CREATE TEMP FUNCTION  active_period_start() AS (TIMESTAMP({active_start_date}));
CREATE TEMP FUNCTION  active_period_end() AS (TIMESTAMP({active_end_date}));

## set the current/ending day to calculate voyage duration up to (GFW) real time AND to ensure no voyage starts after it (also need for events bounding)..
CREATE TEMP FUNCTION  end_day() AS (TIMESTAMP({end_date}));

--------------------------------------
WITH

----------------------------------------------------------
-- Define list of VOIs
----------------------------------------------------------

----------------------------------------------------------
# Create a geometry object for the polygon. Can use either GeoJSON or WKT input.
# copied appropriate GeoJSON text from R variable to match Tyler's code (from GFW map)
----------------------------------------------------------
npg_aoi as (
  SELECT ST_GEOGFROMTEXT(string_field_1) AS geom FROM `world-fishing-827.ocean_shapefiles_all_purpose.NPFC_shape`
),

-- transit AOI to 180 line
dfo_aoi as (
  SELECT
  ST_GEOGFROMTEXT("MultiPolygon (((-179.90452783758004784 34.10511285882622445, -179.86577202662965647 47.9021815571733498, -157.69744816299325407 53.40550671213203771, -144.83051892745606892 57.3585994290741894, -140.56737972291060146 57.04855294147088784, -137.46691484687752904 52.16532076171881016, -128.86312481588578294 46.97204209436343092, -129.87077590059652721 44.56918181543781543, -152.89172760514202309 39.99599612328904641, -171.26198199563791036 36.66299638155350493, -175.99019093158833016 35.81036854064441144, -179.90452783758004784 34.10511285882622445)))") AS transit_aoi_p1,
  ST_GEOGFROMTEXT("MultiPolygon (((179.97224369522007237 34.8899180305720904, 175.11807837368084506 32.75834842829937799, 174.8080318860775435 28.57272084565475723, 145.04356907616022454 27.9526278704481399, 142.40817393153210446 33.06839491590268665, 143.80338312574698989 34.92867384152251731, 145.3536155637635261 38.49420844896053495, 147.36891773318501464 40.81955710598532505, 146.12873178277177999 42.52481278780350493, 145.50863880756517688 45.78030090763821391, 147.6789642207883162 47.64057983325804457, 154.57749856996181848 46.59417293759685919, 165.39036982512709528 53.5314630977208239, 173.91664823421800179 50.3534865997869403, 179.88504312058162782 49.03578902747289447, 179.97224369522007237 34.8899180305720904)))") AS transit_aoi_p2,
  ST_GEOGFROMTEXT("MultiPolygon (((145.00481326520954894 43.0189493774212437, 173.08339829878394767 42.9995714719460409, 173.02526458235831797 29.00872371884684853, 145.00481326520954894 29.04747952979726477, 145.00481326520954894 43.0189493774212437)))") AS primary_area_p1,
  ST_GEOGFROMTEXT("MultiPolygon (((144.63663306118061769 34.91898488878487683, 146.10935387729631429 39.68594963568570932, 165.44850354155252603 52.94043698072704274, 175.44750276675915757 45.26678641254521551, 150.91507443514757369 29.18312486812371276, 144.63663306118061769 34.91898488878487683)))") AS primary_area_p2,
),

----------------------------------------------------------
-- all vessels active in AOI during period of interest from messages table
----------------------------------------------------------
active_vessel_ids AS (
  SELECT DISTINCT
    vessel_id,
    ssvid,
    ST_CONTAINS(a.transit_aoi_p1, ST_GEOGPOINT(v.lon, v.lat)) AS in_transit_aoi_p1,
    ST_CONTAINS(a.transit_aoi_p2, ST_GEOGPOINT(v.lon, v.lat)) AS in_transit_aoi_p2,
    ST_CONTAINS(a.primary_area_p1, ST_GEOGPOINT(v.lon, v.lat)) AS primary_area_p1,
    ST_CONTAINS(a.primary_area_p2, ST_GEOGPOINT(v.lon, v.lat)) AS primary_area_p2
  FROM `world-fishing-827.pipe_ais_v3_published.messages` v CROSS JOIN dfo_aoi a
    WHERE timestamp >= active_period_start() AND timestamp <= active_period_end()
    AND clean_segs IS TRUE
  # Filter to polygon using ST_CONTAINS.
  # You need to create a geometry object for each position using ST_GEOPOINT
  -- AND ST_CONTAINS((SELECT geom FROM AOI), ST_GEOGPOINT(lon, lat))
  AND ST_CONTAINS((SELECT geom FROM npg_aoi), ST_GEOGPOINT(lon, lat))
  )

 SELECT DISTINCT
   av.*,
   id.* EXCEPT(vessel_id)
  FROM active_vessel_ids av
  LEFT JOIN  (
    SELECT
      vessel_id,
      shipname,
      callsign,
      imo,
      gfw_best_flag,
      prod_geartype,
      best_vessel_class,
      registry_vessel_class,
    FROM `pipe_ais_v3_published.product_vessel_info_summary`
    WHERE
      potential_fishing
      OR core_is_bunker
      OR core_is_carrier
      ) id USING (vessel_id)

