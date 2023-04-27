# Databricks notebook source
# MAGIC %md
# MAGIC # Train Model

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

TargetFolder=dbutils.widgets.get("TargetFolder")
df=spark.read.format("csv").option("header","True").load(f"/{TargetFolder}/TelcoCustomerChurn.csv")
display(df)
dfX=df.toPandas()

# COMMAND ----------

TestSize=.15
target_col="Churn"
from sklearn.model_selection import train_test_split
dftrain, dftest = train_test_split(dfX, test_size=TestSize, random_state=42, shuffle=True)

dftest.to_parquet(f"/dbfs/{TargetFolder}/DataSets/dset_test.parquet")
dftrain.to_parquet(f"/dbfs/{TargetFolder}/DataSets/dset_train.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train the model

# COMMAND ----------

import h2o
from h2o.automl import H2OAutoML
h2o.init()

train = h2o.H2OFrame(dftrain.drop(columns=["customerID"]))
test = h2o.H2OFrame(dftest.drop(columns=["customerID"]))
x = train.columns
y = target_col

x.remove(y)

# COMMAND ----------

train[y] = train[y].asfactor()
test[y] = test[y].asfactor()
aml = H2OAutoML(max_models=20, 
                seed=1, 
                exclude_algos=["DeepLearning"], #,"StackedEnsemble"]
                balance_classes=True
               )
aml.train(x=x, y=y, training_frame=train)
# View the AutoML Leaderboard
lb = aml.leaderboard
lb.head(rows=lb.nrows)


# COMMAND ----------

# We want the most performant GBM
selectedModel=""
for index, row in lb.as_data_frame().iterrows():
   print("--->Saving ",row["model_id"], "...")
   if selectedModel=="" and "GBM" in row["model_id"]:
    selectedModel=row["model_id"]
   modelid=row["model_id"]
   model_path = h2o.save_model(model=h2o.get_model(row["model_id"]), path=f"/dbfs/{TargetFolder}/Models", force=True)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Working with a specific model

# COMMAND ----------

#training Metrics
TargetModel=selected_model
modelX=h2o.get_model(TargetModel)
confMatrix=modelX.confusion_matrix()
precision=modelX.precision()
recall=modelX.recall()
with open(f"/dbfs/{TargetFolder}/Metrics/train_ConfusionMatrix.txt", "w") as text_file:
  text_file.write( f"{confMatrix}\nPrecision->{precision}\nRecall->{recall}")

# COMMAND ----------

import matplotlib.pyplot as plt
import json
#ROC
out = modelX.model_performance()
fpr = out.fprs
tpr = out.tprs
fig, ax = plt.subplots(figsize=(8, 8))
ax.plot(fpr,tpr,label=f"Model {TargetModel}, auc="+str(out.auc()))
ax.plot([0, 1], [0, 1], linestyle="--", color="black", label="Random baseline")
ax.set(
    title=f"Train ROC {TargetModel}"
)
ax.legend(loc="upper left")
plt.savefig(f'/dbfs/{TargetFolder}/Metrics/train_roc.png')

# COMMAND ----------

ModelY=h2o.load_model(f"/dbfs/{TargetFolder}/Models/{TargetModel}")

# COMMAND ----------

ModelY.show_summary()

# COMMAND ----------

dfX2=ModelY.predict(test).as_data_frame()

# COMMAND ----------

dfX2

# COMMAND ----------

# Testing what will be the output
print( str(dfX2.iloc[0, 0]) +"|" + str( round(dfX2.iloc[0, 1],2)) + "|" + str( round(dfX2.iloc[0, 2],2)))

# COMMAND ----------

# Saving the Mojo
path = f"/dbfs/{TargetFolder}/Model_POJO/{TargetModel}"
ModelY.save_mojo(path)
