# Databricks notebook source
# MAGIC %md
# MAGIC # Publish to kafka topic

# COMMAND ----------

!pip install --upgrade pip

# COMMAND ----------

import os

kafka_user=os.environ["kafka_user"]
kafka_password=os.environ["kafka_password"]
kafka_server=os.environ["kafka_server"]


# COMMAND ----------



# COMMAND ----------

TargetFolder=dbutils.widgets.get("TargetFolder")

# COMMAND ----------

import csv
import requests
import time
k=0

TopicName="testTopic"
url = f"https://{kafka_server}/webhook?&topic={TopicName}&user={kafka_user}&pass={kafka_password}"

with open(f'/dbfs/{TargetFolder}/TelcoCustomerChurn.csv') as csv_file:
    csv_reader = csv.reader(csv_file)
    line_count = 0
    for row in csv_reader:
        message = ','.join(row)
        headers = {
          'Content-Type': 'text/plain'
        }
        if line_count == 0:
            print(f'Column names are {", ".join(row)}')
            line_count += 1
        else:
            print(f'\t Processing client {row[0]} ')
            line_count += 1
            try:
              response = requests.request("POST", url, headers=headers, data=message)
            except Exception as e:
              print(str(e))
            time.sleep(2)
       


# COMMAND ----------

producer.close()
