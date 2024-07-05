-- Databricks notebook source
-- MAGIC %md
-- MAGIC ![Nimble](https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSybvaX4Q9vdbLP1tW0NKCYMq0k4s2hESYAMg&s)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Bay Area Hospitality Analytics
-- MAGIC > This notebook captures a comprehensive view of the restaurant landscape in **San Francisco**. Each record contains essential information about individual restaurants, enabling data-driven analyses and informed decision-making.
-- MAGIC
-- MAGIC ### Use Cases
-- MAGIC
-- MAGIC 1. **Market Insights**: This dataset provides valuable insights into the **restaurant landscape** in San Francisco. By analyzing categories and ratings, you can identify popular cuisines, trends, and areas with high-rated dining options.
-- MAGIC 2. **Consumer Decision-Making**: For **food enthusiasts** users, this data helps inform their dining choices. High-rated restaurants in specific categories become go-to options, while low-rated ones may be avoided.
-- MAGIC 3. **Business Strategy**: Restaurant owners and managers can leverage this data to refine their **business strategies**. Understanding which categories thrive and how ratings impact customer satisfaction allows them to optimize menus, improve service, and enhance overall dining experiences.

-- COMMAND ----------

USE solutions.nimble_HSP

-- COMMAND ----------

select
  zip,
  rst_title,
  LATITUDE,
  longitude,
  CUISINE_CAT_1 as CUISINE_TYPE,
  RST_ADDRESS AS ADDRESS,
  substring(LATITUDE, 0, 5) || ', ' || substring(LONGITUDE, 0, 5) AS LAT_LONG,
  case
    when price_bucket is null then 'N/A'
    else price_bucket
  end as price_bucket,
  RATING_SCORE
from
  SF_restaurants_sample

-- COMMAND ----------

-- DBTITLE 1,By Zipcode By Cost
select
  CASE
    zip
    WHEN '94102' THEN 'Hayes Valley, Tenderloin, Civic Center, North of Market'
    WHEN '94103' THEN 'South of Market, Mission Bay'
    WHEN '94104' THEN 'Financial District'
    WHEN '94105' THEN 'South Beach, Rincon Hill'
    WHEN '94107' THEN 'Potrero Hill, South Beach, Mission Bay'
    WHEN '94108' THEN 'Chinatown, Nob Hill'
    WHEN '94109' THEN 'Polk Gulch, Russian Hill, Nob Hill, Tenderloin'
    WHEN '94110' THEN 'Inner Mission, Bernal Heights'
    WHEN '94111' THEN 'Financial District, Embarcadero'
    WHEN '94112' THEN 'Ingleside, Balboa Park, Oceanview'
    WHEN '94114' THEN 'Castro, Noe Valley'
    WHEN '94115' THEN 'Western Addition, Japantown, Lower Pacific Heights'
    WHEN '94116' THEN 'Parkside, Taraval, Inner Sunset'
    WHEN '94117' THEN 'Haight-Ashbury, Cole Valley'
    WHEN '94118' THEN 'Richmond District, Inner Richmond'
    WHEN '94121' THEN 'Richmond District, Outer Richmond'
    WHEN '94122' THEN 'Sunset District, Inner Sunset, Outer Sunset'
    WHEN '94123' THEN 'Marina, Cow Hollow'
    WHEN '94124' THEN 'Bayview-Hunters Point'
    WHEN '94127' THEN 'West Portal, Forest Hill'
    WHEN '94129' THEN 'Presidio'
    WHEN '94130' THEN 'Treasure Island'
    WHEN '94131' THEN 'Twin Peaks, Glen Park'
    WHEN '94132' THEN 'Lake Merced'
    WHEN '94133' THEN 'North Beach, Fishermans Wharf'
    WHEN '94134' THEN 'Visitacion Valley, Portola'
    WHEN '94158' THEN 'Mission Bay'
    ELSE 'Unknown Neighborhood'
  END AS Neighborhood,
  case
    when price_bucket is null then 'N/A'
    else price_bucket
  end as price_bucket
from
  SF_restaurants_sample
order by
  Neighborhood

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Superior Extraction Technology that brings the world’s data to your fingertips
-- MAGIC
-- MAGIC > Nimble fully manages the process of finding, extracting, and delivering data that meets your unique business needs, regardless of the source. Our revolutionary AI data pipelines ensure data from even the least accessible sources is smoothly and reliably delivered in AI and BI ready formats,  freeing up your team to focus on gleaning and applying insights.

-- COMMAND ----------

-- DBTITLE 1,20 Leading Zip Codes
select
  CASE
    zip
    WHEN '94102' THEN 'Hayes Valley, Tenderloin, Civic Center, North of Market'
    WHEN '94103' THEN 'South of Market, Mission Bay'
    WHEN '94104' THEN 'Financial District'
    WHEN '94105' THEN 'South Beach, Rincon Hill'
    WHEN '94107' THEN 'Potrero Hill, South Beach, Mission Bay'
    WHEN '94108' THEN 'Chinatown, Nob Hill'
    WHEN '94109' THEN 'Polk Gulch, Russian Hill, Nob Hill, Tenderloin'
    WHEN '94110' THEN 'Inner Mission, Bernal Heights'
    WHEN '94111' THEN 'Financial District, Embarcadero'
    WHEN '94112' THEN 'Ingleside, Balboa Park, Oceanview'
    WHEN '94114' THEN 'Castro, Noe Valley'
    WHEN '94115' THEN 'Western Addition, Japantown, Lower Pacific Heights'
    WHEN '94116' THEN 'Parkside, Taraval, Inner Sunset'
    WHEN '94117' THEN 'Haight-Ashbury, Cole Valley'
    WHEN '94118' THEN 'Richmond District, Inner Richmond'
    WHEN '94121' THEN 'Richmond District, Outer Richmond'
    WHEN '94122' THEN 'Sunset District, Inner Sunset, Outer Sunset'
    WHEN '94123' THEN 'Marina, Cow Hollow'
    WHEN '94124' THEN 'Bayview-Hunters Point'
    WHEN '94127' THEN 'West Portal, Forest Hill'
    WHEN '94129' THEN 'Presidio'
    WHEN '94130' THEN 'Treasure Island'
    WHEN '94131' THEN 'Twin Peaks, Glen Park'
    WHEN '94132' THEN 'Lake Merced'
    WHEN '94133' THEN 'North Beach, Fishermans Wharf'
    WHEN '94134' THEN 'Visitacion Valley, Portola'
    WHEN '94158' THEN 'Mission Bay'
    ELSE 'Unknown Neighborhood'
  END AS neighborhood,
  count(1) as no_of_restaurants
from
  SF_restaurants_sample
group by
  1
order by
  2 desc
limit
  20

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Ready for a Smarter Data Solution?
-- MAGIC > [Book a Call with a Nimble Solutions Architect](https://calendly.com/wilmerr-nimble/intro-of-nimble-)

-- COMMAND ----------

-- DBTITLE 1,Create a Distance UDF
-- MAGIC %python
-- MAGIC import math
-- MAGIC from pyspark.sql.functions import udf
-- MAGIC from pyspark.sql.types import DoubleType
-- MAGIC
-- MAGIC # Define the Haversine formula as a function
-- MAGIC def haversine(lon1, lat1, lon2, lat2):
-- MAGIC     # Convert decimal degrees to radians
-- MAGIC     lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
-- MAGIC     
-- MAGIC     # Haversine formula
-- MAGIC     dlon = lon2 - lon1
-- MAGIC     dlat = lat2 - lat1
-- MAGIC     a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
-- MAGIC     c = 2 * math.asin(math.sqrt(a))
-- MAGIC     
-- MAGIC     # Radius of earth in kilometers is 6371
-- MAGIC     km = 6371 * c
-- MAGIC     return km
-- MAGIC
-- MAGIC haversine_udf = udf(haversine, DoubleType())
-- MAGIC spark.udf.register("haversine_udf", haversine_udf)

-- COMMAND ----------

-- DBTITLE 1,Delivery  Rating vs Dinning-In Rating
WITH ranked_data AS (
  SELECT
    g.latitude AS Dinning_in_LATITUDE,
    u.latitude AS Delivery_LATITUDE,
    g.longitude AS Dinning_in_LONGITUDE,
    u.longitude AS Delivery_LONGITUDE,
    g.title,
    COALESCE(u.RST_TITLE, g.title) AS NAME,
    COALESCE(u.RST_ADDRESS, g.ADDRESS) AS ADDRESS,
    g.CITY,
    g.COUNTRY,
    g.ZIP_CODE,
    CUISINE_CAT_1 as CUISINE,
    PRICE_BUCKET as PRICE_BUCKET,
    COALESCE(CAST(u.PHONE_NUM AS string), g.PHONE_NUMBER) AS PHONE_NUMBER,
    g.rating AS Dinning_in_OVERALL_RATING,
    CAST (
      (
        CASE
          WHEN u.RATING_SCORE = 'NULL' then Null
          else u.RATING_SCORE
        end
      ) AS FLOAT
    ) AS Delivery_RATING_SCORE,
    CAST (
      (
        CASE
          WHEN u.RATING_CNT = 'NULL' then Null
          else u.RATING_CNT
        end
      ) AS FLOAT
    ) AS Delivery_RATING_COUNT,
    ROW_NUMBER() OVER (
      PARTITION BY g.title
      ORDER BY
        u.rating_score DESC,
        u.collection_date DESC
    ) AS row_num
  FROM
    SF_maps_sample g
    INNER JOIN SF_restaurants_sample u ON g.title = u.RST_TITLE
    AND haversine_udf(
      cast(g.longitude as DOUBLE),
      cast(g.latitude as DOUBLE),
      cast(u.longitude as DOUBLE),
      cast(u.latitude as DOUBLE)
    ) <= 30
    Where g.rating is not null and u.RATING_SCORE != 'NULL'
)
SELECT
  Dinning_in_LATITUDE AS LATITUDE,
  Dinning_in_LONGITUDE AS LONGITUDE,
  NAME,
  ADDRESS,
  CITY,
  COUNTRY,
  ZIP_CODE,
  PHONE_NUMBER,
  CUISINE,
  PRICE_BUCKET,
  Dinning_in_OVERALL_RATING,
  Delivery_RATING_SCORE,
  Delivery_RATING_COUNT
FROM
  ranked_data
WHERE
  row_num = 1
ORDER BY
  Dinning_in_LATITUDE,
  Dinning_in_LONGITUDE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dataset Description
-- MAGIC This table provides comprehensive details about restaurants, including their identifiers, location, pricing, ratings, cuisine categories, and contact information. It’s a valuable resource for various analyses, recommendations, and decision-making related to dining experiences.

-- COMMAND ----------

-- DBTITLE 1,Dataset Decription
DESCRIBE SF_restaurants_sample

-- COMMAND ----------

SELECT
  RST_TITLE as Title,
  PRICE_BUCKET,
  RATING_SCORE,
  RATING_CNT,
  LATITUDE,
  LONGITUDE
FROM
  SF_restaurants_sample
