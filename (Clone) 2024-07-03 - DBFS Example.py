# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/AH_Provisional_Diabetes_Death_Counts_for_2020-1.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# Create a view or table
temp_table_name = "Diabetes_Death_Counts"
df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC select * from `Diabetes_Death_Counts`
# MAGIC

# COMMAND ----------

# File location and type
file_location = _c0
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

# Register the DataFrame as a temporary view
df.createOrReplaceTempView("temp_view")

# Save the DataFrame as a table
permanent_table_name = "Diabetes_Death_Counts"
spark.sql(f"CREATE TABLE IF NOT EXISTS {permanent_table_name} USING parquet AS SELECT * FROM temp_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 1: Create a new table with the desired data
# MAGIC CREATE TABLE Diabetes_Death_Counts_New
# MAGIC AS
# MAGIC SELECT _c0, _c1, _c2, _c3, _c4, _c5, _c6, _c7, _c8, _c9, _c10, _c11, _c12, _c13, _c14, _c15
# MAGIC FROM (
# MAGIC   SELECT _c0, _c1, _c2, _c3, _c4, _c5, _c6, _c7, _c8, _c9, _c10, _c11, _c12, _c13, _c14, _c15,
# MAGIC          ROW_NUMBER() OVER (PARTITION BY _c0, _c1, _c2, _c3, _c4 ORDER BY _c0, _c1, _c2, _c3, _c4) AS row_num
# MAGIC   FROM Diabetes_Death_Counts
# MAGIC ) tmp
# MAGIC WHERE row_num = 1;
# MAGIC
# MAGIC -- Step 2: Drop the original table
# MAGIC DROP TABLE Diabetes_Death_Counts;
# MAGIC
# MAGIC -- Step 3: Rename the new table to the original table name
# MAGIC ALTER TABLE Diabetes_Death_Counts_New RENAME TO Diabetes_Death_Counts;

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC select * from `Diabetes_Death_Counts`
# MAGIC

# COMMAND ----------

# Create a temporary view
df.createOrReplaceTempView("temp_view")
# Save the DataFrame as a table
permanent_table_name = "Diabetes_Death_Counts"
spark.sql(f"CREATE TABLE IF NOT EXISTS {permanent_table_name} USING parquet AS SELECT * FROM temp_view")

# COMMAND ----------

import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clean_and_transform_data(input_file):
    try:
        # Create SparkSession
        spark = SparkSession.builder.getOrCreate()
        logging.info("SparkSession created successfully")

        # Load data from a CSV file into a DataFrame
        df = spark.read.csv(input_file, header=True, inferSchema=True)
        logging.info(f"Data loaded successfully from {input_file}")

        # 1. Manejo de valores nulos
        df = df.replace(["null", ""], None)
        logging.info("Null values handled")

        # 2. Conversión de tipos de datos
        numeric_columns = ["COVID19", "Diabetes_uc", "Diabetes_mc", "C19PlusDiabetes", "C19PlusHypertensiveDiseases", 
                   "C19PlusMajorCardiovascularDiseases", "C19PlusHypertensiveDiseasesAndMCVD", 
                   "C19PlusChronicLowerRespiratoryDisease", "C19PlusKidneyDisease", 
                   "C19PlusChronicLiverDiseaseAndCirrhosis", "C19PlusObesity"]

        for col in numeric_columns:
            df = df.withColumn(col, F.col(col).cast(IntegerType()))
        logging.info("Data types converted")

        # 3. Estandarización de nombres de columnas
        df = df.select([F.col(c).alias(c.replace(" ", "_").replace(".", "_").lower()) for c in df.columns])
        logging.info("Column names standardized")

        # 4. Manejo de categorías
        df = df.withColumn("sex", F.when(F.col("sex") == "Female (F)", "Female")
                                  .when(F.col("sex") == "Male (M)", "Male")
                                  .otherwise(F.col("sex")))
        logging.info("Categories handled")

        # 5. Eliminación de filas duplicadas
        df = df.dropDuplicates()
        logging.info("Duplicate rows removed")

        # 6. Creación de nuevas columnas útiles
        df = df.withColumn("date_of_death", F.to_date(F.concat(F.col("date_of_death_year"), 
                                                               F.lit("-"), 
                                                               F.col("date_of_death_month"), 
                                                               F.lit("-01")), "yyyy-MM-dd"))
        logging.info("New columns created")

        # 7. Filtrado de datos
        df = df.filter(~F.concat_ws("", *df.columns).isin(["0" * len(df.columns)]))
        logging.info("Data filtered")

        # Validación de datos
        total_rows = df.count()
        valid_rows = df.filter(F.col("covid19") >= 0).count()
        if valid_rows != total_rows:
            logging.warning(f"Found {total_rows - valid_rows} rows with invalid COVID19 values")

        # Mostrar el esquema actualizado
        logging.info("Updated schema:")
        df.printSchema()

        # Mostrar algunas filas para verificar los cambios
        logging.info("Sample data:")
        df.show(5, truncate=False)

        return df

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    input_file = "dbfs:/FileStore/tables/AH_Provisional_Diabetes_Death_Counts_for_2020-1.csv"
    clean_and_transform_data(input_file)

# COMMAND ----------



# COMMAND ----------


