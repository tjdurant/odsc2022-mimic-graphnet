from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Window
import os
import numpy as np
from pyspark.sql.functions import udf
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col
from statistics import mode

# Chloride, Blood = 50902
# Creatinine, Blood = 50912
# Hemglobin, Blood = 51222
# Potassium, Blood = 50971
# Sodium, Blood = 50983
# Urea Nitrogen, Blood = 51006
# WBC Count, Blood = 51300
# lab_items.filter(
#     lab_items.LABEL.contains("WBC")
# ).show()

# create User Defined Function for calculating mode of array
array_mode = udf(lambda x: float(mode(x)), FloatType())

# Create spark session, should load any necessary config per your environment requirements
# spark = SparkSession("mimic-extractor")
spark = SparkSession.builder.getOrCreate() # use if running locally

# Set base path to MIMIC data here. If driver/config loaded, will work with
# cloud storage endpoints as well
mimic_base_path = os.environ['MIMIC_PATH']

lab_events = spark.read.csv(os.path.join(mimic_base_path, "LABEVENTS.csv"), header=True)
lab_items = spark.read.csv(os.path.join(mimic_base_path, "D_LABITEMS.csv"), header=True)

lab_events_int = lab_events.na.fill("0", ['FLAG']).replace("abnormal", "1", ["FLAG"])
lab_events_int = lab_events_int.withColumn("FLAG",lab_events_int.FLAG.cast('int'))

admit_lab_events = lab_events_int.filter(
    (col("ITEMID") == "50902") &
    (col("SUBJECT_ID") == "23")).groupBy("HADM_ID").agg(
F.collect_list(lab_events_int.FLAG).alias("lab_mode"))

admit_lab_events_mode = admit_lab_events.withColumn('lab_mode', array_mode("lab_mode"))

admit_lab_events_mode.show()

print("DONE")