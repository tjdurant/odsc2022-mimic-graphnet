from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Window
import os
import pyspark.sql.functions as F
from pyspark.sql.functions import col


# Create spark session, should load any necessary config per your environment requirements
spark = SparkSession("mimic-extractor")

# Set base path to MIMIC data here. If driver/config loaded, will work with
# cloud storage endpoints as well
mimic_base_path = ""

icustays = spark.read.csv(os.path.join(mimic_base_path, "ICUSTAYS.csv"), header=True)
admissions = spark.read.csv(os.path.join(mimic_base_path, "ADMISSIONS.csv"), header=True)
patients = spark.read.csv(os.path.join(mimic_base_path, "PATIENTS.csv"), header=True)
diagnoses = spark.read.csv(os.path.join(mimic_base_path, "DIAGNOSES_ICD.csv"), header=True)

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
).drop("DOB", "ADMITTIME", "age_at_admit_raw")

# Join admits with last ICU stay, calculate remaining LOS post-ICU discharge
pt_icu_admits = pt_admits.join(
    last_icustay.drop("SUBJECT_ID"),
    "HADM_ID",
    how="left"
).drop("ICUSTAY_ID").withColumn(
    "posticu_los",
    F.datediff(col("DISCHTIME"), col("OUTTIME"))
).filter(
    col("posticu_los").isNotNull()
).drop("OUTTIME", "DISCHTIME")

# Join with diagnoses
pt_icu_admits_dxs = pt_icu_admits.join(
    grouped_dx,
    "HADM_ID",
    how="left"
)

# Write to single CSV with header
pt_icu_admits_dxs.coalesce(1).write.csv("mimic-posticu-los", header=True, mode="overwrite")