from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Window
import os
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
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

icustays = spark.read.csv(os.path.join(mimic_base_path, "ICUSTAYS.csv"), header=True)
admissions = spark.read.csv(os.path.join(mimic_base_path, "ADMISSIONS.csv"), header=True)
patients = spark.read.csv(os.path.join(mimic_base_path, "PATIENTS.csv"), header=True)
diagnoses = spark.read.csv(os.path.join(mimic_base_path, "DIAGNOSES_ICD.csv"), header=True)
lab_events = spark.read.csv(os.path.join(mimic_base_path, "LABEVENTS.csv"), header=True)
lab_items = spark.read.csv(os.path.join(mimic_base_path, "D_LABITEMS.csv"), header=True)

# Convert lab flags (i.e., normal/abnormal) to int() (i.e., 0/1)
lab_events_int = lab_events.na.fill("0", ['FLAG']).replace("abnormal", "1", ["FLAG"])
lab_events_int = lab_events_int.withColumn("FLAG",lab_events_int.FLAG.cast('int'))

# Concatenate list of diagnoses to single string column (semicolon separated)
grouped_dx = diagnoses.drop(
    "ROW_ID",
    "SUBJECT_ID",
    "SEQ_NUM"
).groupBy("HADM_ID").agg(
    F.concat_ws(";", F.collect_list(col("ICD9_CODE"))).alias("diagnoses")
)

# Identify last ICU stay for the admission
w1 = Window.partitionBy(
    "HADM_ID"
).orderBy(
    "OUTTIME"
).rowsBetween(
    Window.unboundedPreceding, Window.unboundedFollowing
)

last_icustay = icustays.select(
    "*", 
    F.first('ROW_ID').over(w1).alias("first_id"),
    F.last('ROW_ID').over(w1).alias("last_id")
).filter(
    col("ROW_ID") == col("last_id")
).drop(
    "first_id",
    "last_id",
    "ROW_ID",
    "DBSOURCE",
    "FIRST_CAREUNIT",
    "FIRST_WARDID",
    "LAST_WARDID",
    "INTIME",
    "LOS"
)

# Load admits/patients and create calculated columns for age at admit (group >90yo together)
# Only keep patients who were alive at time of discharge
pt_admits = admissions.filter(col("HOSPITAL_EXPIRE_FLAG") == 0).drop(
    "ROW_ID",
    "DEATHTIME",
    "ADMISSION_TYPE",
    "ADMISSION_LOCATION",
    "DISCHARGE_LOCATION",
    "INSURANCE",
    "LANGUAGE",
    "RELIGION",
    "MARITAL_STATUS",
    "EDREGTIME",
    "EDOUTTIME",
    "DIAGNOSIS",
    "HOSPITAL_EXPIRE_FLAG",
    "HAS_CHARTEVENTS_DATA"
).join(
    patients.drop("ROW_ID", "DOD", "DOD_HOSP", "DOD_SSN", "EXPIRE_FLAG"),
    "SUBJECT_ID",
    how="left"
).withColumn(
    "age_at_admit_raw",
    F.datediff(col("ADMITTIME"), col("DOB")) / 365
).withColumn(
    "age_at_admit", 
    F.when(
        col("age_at_admit_raw") >= 90, 90
    )
    .otherwise(
        col("age_at_admit_raw")
    )
).filter(
    col("age_at_admit") >= 18
).drop("DOB", "age_at_admit_raw")

# Join admits with last ICU stay, calculate total LOS and remaining LOS post-ICU discharge
pt_icu_admits = pt_admits.join(
    last_icustay.drop("SUBJECT_ID"),
    "HADM_ID",
    how="left"
).drop("ICUSTAY_ID").withColumn(
    "posticu_los",
    (F.to_timestamp(col("DISCHTIME")).cast("long") - F.to_timestamp(col("OUTTIME")).cast("long"))/60.0/60.0/24.0
).withColumn(
    "total_los",
    (F.to_timestamp(col("DISCHTIME")).cast("long") - F.to_timestamp(col("ADMITTIME")).cast("long"))/60.0/60.0/24.0
).filter(
    col("posticu_los").isNotNull()
).drop("ADMITTIME", "OUTTIME", "DISCHTIME")

# Join with diagnoses
pt_icu_admits_dxs = pt_icu_admits.join(
    grouped_dx,
    "HADM_ID",
    how="left"
)

# Create dataframe of unique HADM_ID in LABEVENTS to join with lab_modes later
admit_lab_events_mode = lab_events.select(col("HADM_ID")).filter(col("HADM_ID").isNotNull()).distinct()

# iterate through list of labs and find mode for each HADM_ID
for lab_item_id in rothman_labid_list:

    # retreive the label for a given lab ITEMID
    lab_name = str(lab_items.filter(lab_items.ITEMID == lab_item_id).first()[2]).lower().replace(" ","_")
    new_lab_column = f"{lab_name}_lab_mode"

    # Collect list of lab_item_id for each HADM_ID
    admit_lab_events = lab_events_int.filter((col("ITEMID") == lab_item_id)
    ).groupBy(
        "HADM_ID"
    ).agg(F.collect_list(lab_events_int.FLAG
    ).alias(new_lab_column))

    # find mode of lab_item_id list and store in tmp df
    tmp_admit_lab_events_mode = admit_lab_events.withColumn(new_lab_column, array_mode(new_lab_column))

    # join lab_item_id mode df to df of unique HADM_IDs
    admit_lab_events_mode = admit_lab_events_mode.join(
        tmp_admit_lab_events_mode,
        "HADM_ID",
        how="left"
    )

# Join with lab results
pt_icu_admits_dxs_lxs = pt_icu_admits_dxs.join(
    admit_lab_events_mode,
    "HADM_ID",
    how="left"
)


# Write to single CSV with header
pt_icu_admits_dxs_lxs.coalesce(1).write.csv("mimic-posticu-los", header=True, mode="overwrite")