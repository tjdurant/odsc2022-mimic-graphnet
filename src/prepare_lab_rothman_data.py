from itertools import count
from pandas import isnull
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Window
import os
import numpy as np
from pyspark.sql.functions import udf
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col
from statistics import mode, StatisticsError

# Chloride, Blood = 50902
# Creatinine, Blood = 50912
# Hemglobin, Blood = 51222
# Potassium, Blood = 50971
# Sodium, Blood = 50983
# Urea Nitrogen, Blood = 51006
# WBC Count, Blood = 51301
rothman_labid_list = [
    "50902",
    "50912",
    "51222",
    "50971",
    "50983",
    "51006",
    "51301"
]

def find_mode(value_list):
    try:
        return round(float(mode(value_list)),2)
    except StatisticsError as e:
        return None

# create User Defined Function for calculating mode of array
# array_mode = udf(lambda x: float(mode(x)), FloatType())
array_mode = udf(find_mode, FloatType())

# Create spark session, should load any necessary config per your environment requirements
# spark = SparkSession("mimic-extractor")
spark = SparkSession.builder.getOrCreate() # use if running locally

# Set base path to MIMIC data here. If driver/config loaded, will work with
# cloud storage endpoints as well
mimic_base_path = os.environ['MIMIC_PATH']

lab_events = spark.read.csv(os.path.join(mimic_base_path, "LABEVENTS.csv"), header=True)
lab_items = spark.read.csv(os.path.join(mimic_base_path, "D_LABITEMS.csv"), header=True)
# lab_items.filter(col('LABEL').rlike('White')).show()

lab_events_int = lab_events.na.fill("0", ['FLAG']).replace("abnormal", "1", ["FLAG"])
lab_events_int = lab_events_int.withColumn("FLAG",lab_events_int.FLAG.cast('int'))


# lab_item_id = "51222"
admit_lab_events_mode = lab_events.select(col("HADM_ID")).filter(col("HADM_ID").isNotNull()).distinct()
for lab_item_id in rothman_labid_list:

    # If we use the lambda udf, and don't throw exceptions for mode, 
    # when we get to 51222, we throw a weird error when we do .show() or write.csv()
    # from the statistics module saying we can't do mode on None data

    # if lab_item_id != "51300":
    #     continue

    lab_name = str(lab_items.filter(lab_items.ITEMID == lab_item_id).first()[2]).lower().replace(" ","_")
    new_lab_column = f"{lab_name}_lab_mode"

    admit_lab_events = lab_events_int.filter(
        (col("ITEMID") == lab_item_id)).groupBy("HADM_ID").agg(
    F.collect_list(lab_events_int.FLAG).alias(new_lab_column))

    tmp_admit_lab_events_mode = admit_lab_events.withColumn(new_lab_column, array_mode(new_lab_column))

    admit_lab_events_mode = admit_lab_events_mode.join(
        tmp_admit_lab_events_mode,
        "HADM_ID",
        how="left"
    )

admit_lab_events_mode.show(n=50)
print("DONE")