-- Views to transform marketplace data in pipeline
USE ROLE accountadmin;
USE SCHEMA quickstart_prod.silver;

/*
To join the flight and location focused tables 
we need to cross the gap between the airport and cities domains. 
For this we make use of a Snowpark Python UDF. 
What's really cool is that Snowpark allows us to define a vectorized UDF 
making the processing super efficient as we don’t have to invoke the 
function on each row individually!

To compute the mapping between airports and cities, 
we use SnowflakeFile to read a JSON list from the pyairports package. 
The SnowflakeFile class provides dynamic file access, to stream files of any size.
 */
CREATE OR REPLACE FUNCTION get_city_for_airport(iata varchar)
RETURNS VARCHAR
LANGUAGE python
RUNTIME_VERSION = '3.11'
HANDLER = 'get_city_for_airport'
PACKAGES = ('snowflake-snowpark-python')
as $$
from snowflake.snowpark.files import SnowflakeFile
from _snowflake import vectorized
import pandas
import json
@vectorized(input=pandas.DataFrame)
def get_city_for_airport(df):
  airport_list = json.loads(SnowflakeFile.open("@bronze.raw/airport_list.json", 'r', require_scoped_url = False).read())
  airports = {airport[3]: airport[1] for airport in airport_list}
  return df[0].apply(lambda iata: airports.get(iata.upper()))
$$;

/*
To mangle the data into a more usable form, 
we make use of views to not materialize the marketplace data 
and avoid the corresponding storage costs. 
 */

-- We are interested in the per seat carbon emissions. 
-- To obtain these, we need to divide the emission data by the number of seats in the airplane.
CREATE OR REPLACE VIEW flight_emissions AS
  SELECT departure_airport, arrival_airport, avg(estimated_co2_total_tonnes / seats) * 1000 AS co2_emissions_kg_per_person
  FROM oag_flight_emissions_data_sample.public.estimated_emissions_schedules_sample
  WHERE seats != 0 
  AND estimated_co2_total_tonnes IS NOT NULL
  GROUP BY departure_airport, arrival_airport;

-- To avoid unreliable flight connections, we compute the fraction of flights that arrive 
-- early or on time from the flight status data provided by OAG.
CREATE OR REPLACE VIEW flight_punctuality AS
  SELECT departure_iata_airport_code, arrival_iata_airport_code, COUNT(CASE WHEN arrival_actual_ingate_timeliness IN ('OnTime', 'Early') THEN 1 END) / COUNT(*) * 100 AS punctual_pct
  FROM oag_flight_status_data_sample.public.flight_status_latest_sample
  WHERE arrival_actual_ingate_timeliness IS NOT NULL
  GROUP BY departure_iata_airport_code, arrival_iata_airport_code;

-- When joining the flight emissions with the punctuality view, 
-- we filter for flights starting from the airport closest to where we live. 
-- This information is provided in the tiny JSON file data/home.json which we query directly in the view.
CREATE OR REPLACE VIEW flights_from_home AS 
  SELECT 
    departure_airport, 
    arrival_airport, 
    get_city_for_airport(arrival_airport) arrival_city,  
    co2_emissions_kg_per_person, 
    punctual_pct,
  FROM flight_emissions
  JOIN flight_punctuality ON departure_airport = departure_iata_airport_code 
    AND arrival_airport = arrival_iata_airport_code
  WHERE departure_airport = (
    SELECT $1:airport 
    FROM @quickstart_common.public.quickstart_repo/branches/main/data/home.json 
    (FILE_FORMAT => bronze.json_format)
  );

-- Weather Source provides a weather forecast for the upcoming two weeks. 
-- As the free versions of the data sets we use do not cover the entire globe, 
-- we limit our pipeline to zip codes inside the US and compute the average 
-- temperature, humidity, precipitation probability and cloud coverage.
CREATE OR REPLACE VIEW weather_forecast AS 
  SELECT postal_code, avg(avg_temperature_air_2m_f) avg_temperature_air_f, avg(avg_humidity_relative_2m_pct) avg_relative_humidity_pct, avg(avg_cloud_cover_tot_pct) avg_cloud_cover_pct, avg(probability_of_precipitation_pct) precipitation_probability_pct
  FROM global_weather__climate_data_for_bi.standard_tile.forecast_day
  WHERE country = 'US'
  GROUP BY postal_code;

-- We use the data provided by Cybersyn to limit our pipeline to 
-- US cities with atleast 100k residents to enjoy all the benefits a big city provides during our vacation.
CREATE OR REPLACE VIEW major_us_cities AS 
  SELECT geo.geo_id, geo.geo_name, max(ts.value) total_population
  FROM frostbyte_cs_public.cybersyn.datacommons_timeseries ts
  JOIN frostbyte_cs_public.cybersyn.geography_index geo ON ts.geo_id = geo.geo_id
  JOIN frostbyte_cs_public.cybersyn.geography_relationships geo_rel ON geo_rel.related_geo_id = geo.geo_id
  WHERE TRUE
    AND ts.variable_name = 'Total Population, census.gov'
    AND date >= '2020-01-01'
    AND geo.level = 'City'
    AND geo_rel.geo_id = 'country/USA'
    AND value > 100000
  GROUP BY geo.geo_id, geo.geo_name
  ORDER BY total_population DESC;

-- Using the geography relationships provided by Cybersyn we collect all the zip codes belonging to a city.
CREATE OR REPLACE VIEW zip_codes_in_city AS 
  SELECT city.geo_id city_geo_id, city.geo_name city_geo_name, city.related_geo_id zip_geo_id, city.related_geo_name zip_geo_name
  FROM us_points_of_interest__addresses.cybersyn.geography_relationships country
  JOIN us_points_of_interest__addresses.cybersyn.geography_relationships city ON country.related_geo_id = city.geo_id
  WHERE TRUE
    AND country.geo_id = 'country/USA'
    AND city.level = 'City'
    AND city.related_level = 'CensusZipCodeTabulationArea'
  ORDER BY city_geo_id;

CREATE OR REPLACE VIEW weather_joined_with_major_cities AS 
  SELECT 
    city.geo_id, 
    city.geo_name, city.total_population,
    avg(avg_temperature_air_f) avg_temperature_air_f,
    avg(avg_relative_humidity_pct) avg_relative_humidity_pct,
    avg(avg_cloud_cover_pct) avg_cloud_cover_pct,
    avg(precipitation_probability_pct) precipitation_probability_pct
  FROM major_us_cities city
  JOIN zip_codes_in_city zip ON city.geo_id = zip.city_geo_id
  JOIN weather_forecast weather ON zip.zip_geo_name = weather.postal_code
  GROUP BY city.geo_id, city.geo_name, city.total_population;

CREATE OR REPLACE VIEW attractions AS
  SELECT 
    city.geo_id,
    city.geo_name,
    COUNT(CASE WHEN category_main = 'Aquarium' THEN 1 END) aquarium_cnt,
    COUNT(CASE WHEN category_main = 'Zoo' THEN 1 END) zoo_cnt,
    COUNT(CASE WHEN category_main = 'Korean Restaurant' THEN 1 END) korean_restaurant_cnt,
  FROM us_points_of_interest__addresses.cybersyn.point_of_interest_index poi
  JOIN us_points_of_interest__addresses.cybersyn.point_of_interest_addresses_relationships poi_add ON poi_add.poi_id = poi.poi_id
  JOIN us_points_of_interest__addresses.cybersyn.us_addresses address ON address.address_id = poi_add.address_id
  JOIN major_us_cities city ON city.geo_id = address.id_city
  WHERE TRUE
      AND category_main IN ('Aquarium', 'Zoo', 'Korean Restaurant')
      AND id_country = 'country/USA'
  GROUP BY city.geo_id, city.geo_name;