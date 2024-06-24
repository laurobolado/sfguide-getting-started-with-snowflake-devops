USE ROLE accountadmin;
USE SCHEMA quickstart_{{environment}}.gold;

-- declarative target table of pipeline
CREATE OR ALTER TABLE vacation_spots (
    city VARCHAR
  , airport VARCHAR
  , co2_emissions_kg_per_person FLOAT
  , punctual_pct FLOAT
  , avg_temperature_air_f FLOAT
  , avg_relative_humidity_pct FLOAT
  , avg_cloud_cover_pct FLOAT
  , precipitation_probability_pct FLOAT
  -- STEP 5: INSERT CHANGES HERE
  , aquarium_cnt INT
  , zoo_cnt INT
  , korean_restaurant_cnt INT
) data_retention_time_in_days = {{retention_time}};

-- task to merge pipeline results into target table
CREATE OR ALTER TASK vacation_spots_update
  SCHEDULE = '1440 minute'
  WAREHOUSE = 'quickstart_wh'
  AS MERGE INTO vacation_spots USING (
    SELECT *
    FROM silver.flights_from_home flight
    JOIN silver.weather_joined_with_major_cities city ON city.geo_name = flight.arrival_city
    -- STEP 5: INSERT CHANGES HERE
    JOIN silver.attractions att ON att.geo_name = city.geo_name
  ) AS harmonized_vacation_spots ON vacation_spots.city = harmonized_vacation_spots.arrival_city 
    AND vacation_spots.airport = harmonized_vacation_spots.arrival_airport
  WHEN MATCHED THEN
    UPDATE SET
        vacation_spots.co2_emissions_kg_per_person = harmonized_vacation_spots.co2_emissions_kg_per_person
      , vacation_spots.punctual_pct = harmonized_vacation_spots.punctual_pct
      , vacation_spots.avg_temperature_air_f = harmonized_vacation_spots.avg_temperature_air_f
      , vacation_spots.avg_relative_humidity_pct = harmonized_vacation_spots.avg_relative_humidity_pct
      , vacation_spots.avg_cloud_cover_pct = harmonized_vacation_spots.avg_cloud_cover_pct
      , vacation_spots.precipitation_probability_pct = harmonized_vacation_spots.precipitation_probability_pct
      -- STEP 5: INSERT CHANGES HERE
      , vacation_spots.aquarium_cnt = harmonized_vacation_spots.aquarium_cnt
      , vacation_spots.zoo_cnt = harmonized_vacation_spots.zoo_cnt
      , vacation_spots.korean_restaurant_cnt = harmonized_vacation_spots.korean_restaurant_cnt
  WHEN NOT MATCHED THEN 
    INSERT VALUES (
        harmonized_vacation_spots.arrival_city
      , harmonized_vacation_spots.arrival_airport
      , harmonized_vacation_spots.co2_emissions_kg_per_person
      , harmonized_vacation_spots.punctual_pct
      , harmonized_vacation_spots.avg_temperature_air_f
      , harmonized_vacation_spots.avg_relative_humidity_pct
      , harmonized_vacation_spots.avg_cloud_cover_pct
      , harmonized_vacation_spots.precipitation_probability_pct
      -- STEP 5: INSERT CHANGES HERE
      , harmonized_vacation_spots.aquarium_cnt
      , harmonized_vacation_spots.zoo_cnt
      , harmonized_vacation_spots.korean_restaurant_cnt
    );

-- task to select perfect vacation spot and send email with vacation plan
-- NOTE: NOT ALL CORTEX ML MODELS MAY BE AVAILABLE ON ALL DEPLOYMENTS
CREATE OR ALTER TASK email_notification
  WAREHOUSE = 'quickstart_wh'
  AFTER vacation_spots_update
  AS 
    BEGIN
      LET options VARCHAR := (
        SELECT TO_VARCHAR(ARRAY_AGG(OBJECT_CONSTRUCT(*)))
        FROM vacation_spots
        WHERE TRUE
          AND punctual_pct >= 50
          AND avg_temperature_air_f >= 70
          -- STEP 5: INSERT CHANGES HERE
          AND korean_restaurant_cnt > 0
          AND (zoo_cnt > 0 OR aquarium_cnt > 0)
        LIMIT 10);

      IF (:options = '[]') THEN
        CALL SYSTEM$SEND_EMAIL(
            'email_integration',
            'lauro.bolado@perficient.com', -- INSERT YOUR EMAIL HERE
            'New data successfully processed: No suitable vacation spots found.',
            'The query did not return any results. Consider adjusting your filters.');
      END IF;

      LET query VARCHAR := 'Considering the data provided below in JSON format, pick the best city for a family vacation in summer?
      Explain your choise, offer a short description of the location and provide tips on what to pack for the vacation considering the weather conditions? 
      Finally, could you provide a detailed plan of daily activities for a one week long vacation covering the highlights of the chosen destination?\n\n';
      
      LET response VARCHAR := (SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-7b', :query || :options));

      CALL SYSTEM$SEND_EMAIL(
        'email_integration',
        'lauro.bolado@perficient.com', -- INSERT YOUR EMAIL HERE
        'New data successfully processed: The perfect place for your summer vacation has been found.',
        :response);
    EXCEPTION
        WHEN EXPRESSION_ERROR THEN
            CALL SYSTEM$SEND_EMAIL(
            'email_integration',
            'lauro.bolado@perficient.com', -- INSERT YOUR EMAIL HERE
            'New data successfully processed: Cortex LLM function inaccessible.',
            'It appears that the Cortex LLM functions are not available in your region');
    END;

-- resume follow-up task so it is included in DAG runs
-- don't resume the root task so the regular schedule doesn't get invoked
ALTER TASK email_notification RESUME;

-- manually initiate a full execution of the DAG
EXECUTE TASK vacation_spots_update;

/*
-- SQL commands to monitor the progress of tasks

-- Get a list of tasks
SHOW TASKS;

-- Task execution history in the past day
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START=>DATEADD('DAY',-1,CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 100))
ORDER BY SCHEDULED_TIME DESC;

-- Scheduled task runs
SELECT
    TIMESTAMPDIFF(SECOND, CURRENT_TIMESTAMP, SCHEDULED_TIME) NEXT_RUN,
    SCHEDULED_TIME,
    NAME,
    STATE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE STATE = 'SCHEDULED'
ORDER BY COMPLETED_TIME DESC;
*/
