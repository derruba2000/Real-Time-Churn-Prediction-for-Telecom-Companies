# Databricks notebook source
# MAGIC %md
# MAGIC #Read from Kafka topic and write file

# COMMAND ----------

TargetFolder=dbutils.widgets.get("TargetFolder")

# COMMAND ----------


folder_path = f'/{TargetFolder}/DataInput'
# Remove all CSV files inside the folder_path directory
for file in dbutils.fs.ls(folder_path):
  if file.name.endswith(".csv"):
    dbutils.fs.rm(file.path, True)

# COMMAND ----------

!pip install --upgrade pip

# COMMAND ----------

!pip install --upgrade kafka-python

# COMMAND ----------

import os

kafka_user=os.environ["kafka_user"]
kafka_password=os.environ["kafka_password"]
kafka_server=os.environ["kafka_server"]

# COMMAND ----------

from kafka import KafkaConsumer
 
consumer = KafkaConsumer(
  bootstrap_servers=[f'{kafka_server}:9092'],
  sasl_mechanism='SCRAM-SHA-256',
  security_protocol='SASL_SSL',
  sasl_plain_username=kafka_user,
  sasl_plain_password=kafka_password,
  group_id='$GROUP_NAME',
  auto_offset_reset='earliest',
)

# COMMAND ----------

topicName="testTopic"

consumer.subscribe(topics=[topicName])
line_count = 0
for message in consumer:
    print(f"> Reading message {line_count}")
    line_count += 1
    f = open(f"/dbfs/{TargetFolder}/DataInput/file_line_{line_count}.csv", "w")
    f.write(message.value.decode('utf-8'))
    f.close()
print(f'Processed {line_count} lines.')

# COMMAND ----------

consumer.close()
