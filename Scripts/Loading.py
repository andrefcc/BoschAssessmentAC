#Simulation: Databricks Delta Lake Load

#Location where Gold tables will be saved in Delta Lake format in a gneneric default mount path in Databricks
import os
area = 'ASSESSMENT'
project = 'vehicle_analytics'
db_location = f'dbfs:/mnt/{area}/{project}/databases/'
db_processed = 'gold'

spark.sql(f'create database if not exists {db_processed} location "{os.path.join(db_location, db_processed)}.db"')

# Paths to Gold files hypothetically uploaded to dbfs
gold_base_path = "dbfs:/mnt/ASSESSMENT/Vehicle_Analytics/gold/"

# Read Gold parquet files into Spark
safety_summary = spark.read.parquet(f"{gold_base_path}safety_summary.parquet")
light_vehicle_detail = spark.read.parquet(f"{gold_base_path}light_vehicle_detail.parquet")
fuel_summary = spark.read.parquet(f"{gold_base_path}fuel_summary.parquet")

#Save tables in gold db
#Each saprk table is saved as a Delta Lake table, with overwrite to replace any existing data, and overwriteSchema to allow schema changes if needed
safety_summary.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable('gold.safety_summary')
light_vehicle_detail.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable('gold.light_vehicle_detail')
fuel_summary.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable('gold.fuel_summary')