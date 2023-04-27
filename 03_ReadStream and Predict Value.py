# Databricks notebook source
# MAGIC %md
# MAGIC # Reads from Stream and Predict Value

# COMMAND ----------

ModelX=dbutils.widgets.get("Model")
TargetFolder=dbutils.widgets.get("TargetFolder")

# COMMAND ----------

dbutils.fs.rm(f"/{TargetFolder}/DataOutput/", True)
dbutils.fs.rm(f"/{TargetFolder}/Checkpoint/", True)
dbutils.fs.mkdirs(f"/{TargetFolder}/DataOutput/")
dbutils.fs.mkdirs(f"/{TargetFolder}/Checkpoint/")

# COMMAND ----------

import h2o
from h2o.automl import H2OAutoML

# COMMAND ----------

from pyspark.sql.functions import udf, struct
from pyspark.sql.types import *
import pandas as pd

# Define a function that returns a string
def predict_udf_func(row,ModelX,TargetFolder):
    columnNames = [
        "gender",
        "SeniorCitizen",
        "Partner",
        "Dependents",
        "tenure",
        "PhoneService",
        "MultipleLines",
        "InternetService",
        "OnlineSecurity",
        "OnlineBackup",
        "DeviceProtection",
        "TechSupport",
        "StreamingTV",
        "StreamingMovies",
        "Contract",
        "PaperlessBilling",
        "PaymentMethod",
        "MonthlyCharges",
        "TotalCharges",
    ]
    df1 = pd.DataFrame(
        [
            [
                str(row["gender"]),
                str(row["SeniorCitizen"]),
                str(row["Partner"]),
                str(row["Dependents"]),
                str(row["tenure"]),
                str(row["PhoneService"]),
                str(row["MultipleLines"]),
                str(row["InternetService"]),
                str(row["OnlineSecurity"]),
                str(row["OnlineBackup"]),
                str(row["DeviceProtection"]),
                str(row["TechSupport"]),
                str(row["StreamingTV"]),
                str(row["StreamingMovies"]),
                str(row["Contract"]),
                str(row["PaperlessBilling"]),
                str(row["PaymentMethod"]),
                str(row["MonthlyCharges"]),
                str(row["TotalCharges"]),
            ]
        ],
        columns=columnNames,
    )


    h2o.init()
    # Convert the Pandas DataFrame to an H2O Frame
    hf = h2o.H2OFrame(df1)
    # Make predictions with the H2O model
    ModelY = h2o.load_model(f"/dbfs/{TargetFolder}/Models/{ModelX}")
    # Convert the predictions to a list
    dfX2 = ModelY.predict(hf).as_data_frame()
    
    return  str(dfX2.iloc[0, 0]) +"|" + str( round(dfX2.iloc[0, 1],2)) + "|" + str( round(dfX2.iloc[0, 2],2)) 

# Define the UDF
my_udf = udf(lambda row: predict_udf_func(row, ModelX, TargetFolder), StringType())



# COMMAND ----------

schema = StructType(
    [
        StructField("customerID", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("SeniorCitizen", DoubleType(), True),
        StructField("Partner", StringType(), True),
        StructField("Dependents", StringType(), True),
        StructField("tenure", DoubleType(), True),
        StructField("PhoneService", StringType(), True),
        StructField("MultipleLines", StringType(), True),
        StructField("InternetService", StringType(), True),
        StructField("OnlineSecurity", StringType(), True),
        StructField("OnlineBackup", StringType(), True),
        StructField("DeviceProtection", StringType(), True),
        StructField("TechSupport", StringType(), True),
        StructField("StreamingTV", StringType(), True),
        StructField("StreamingMovies", StringType(), True),
        StructField("Contract", StringType(), True),
        StructField("PaperlessBilling", StringType(), True),
        StructField("PaymentMethod", StringType(), True),
        StructField("MonthlyCharges", DoubleType(), True),
        StructField("TotalCharges", DoubleType(), True),
        StructField("Churn", StringType(), True),
    ]
)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName("streaming_csv_to_delta").getOrCreate()

# Read the CSV data as a stream
csv_stream = (
    spark.readStream.format("csv")
    .schema(schema)
    .option("header", "false")
    .load(f"/{TargetFolder}/DataInput")
)


csv_stream2 = csv_stream.withColumn(
    "Prediction",
      my_udf(struct([col(c) for c in schema.names]))
)

# Convert the CSV data to a Delta table
delta_stream = (
    csv_stream2.select("customerID", "Churn", "SeniorCitizen","Prediction")
    .writeStream.format("delta")
    .option("checkpointLocation", f"/{TargetFolder}/Checkpoint")
    .option("path", f"/{TargetFolder}/DataOutput/table_X")
    .start()
)

delta_stream.awaitTermination()
