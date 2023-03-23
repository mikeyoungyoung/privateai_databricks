# Databricks notebook source
# MAGIC %md 
# MAGIC # Limitations and Backgound
# MAGIC 
# MAGIC PrivateAI PII/PHI NER models work best when entities have further context around then. For example `My SIN is 991 834 988` will return `My SIN is [SIN_1] `, but passing in just `991 834 988` may return `[PHONENUMBER_1`. To use privateAI APIs on short form text it is better to pass in a column name and then the value like `SIN 901 934 092`.
# MAGIC </br>
# MAGIC </br>
# MAGIC However, there are obvisouly easier ways to handle columns dedicated to storing individual PII/PHI token and dropping or tokenizing the column may be the most appropriate

# COMMAND ----------

import requests
import json

# COMMAND ----------

API_KEY = dbutils.secrets.get(scope = "privateai", key = "demoapi")
headers = {"Content-Type": "application/json",
            "X-API-KEY": API_KEY}
url = "https://app-deid-staging-east001.azurewebsites.net/v3/process/text"

# COMMAND ----------

def redact_text(text):
    payload = {
        "text":[text],
        "entity_detection":{"accuracy":"high"}
    }
    response = requests.post(url, json=payload, headers=headers)
    # print(response.json()[0]['processed_text'] )
    return response.json()[0]['processed_text']
def anon_text(text):

    payload = {
        "text": [text],
        "processed_text":{"type": "SYNTHETIC", "synthetic_entity_accuracy": "standard"}
    }
    response = requests.post(url, json=payload, headers=headers)
    # print(response.json()[0]['processed_text'] )
    return response.json()[0]['processed_text']

# COMMAND ----------

print(redact_text("My name is Mike and my phone number is 981 383 4923"))
print(anon_text("My name is Mike and my phone number is 981 383 4923"))

# COMMAND ----------


from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# Converting function to UDF 
redact_textUDF = udf(lambda z: redact_text(z),StringType())
anon_textUDF = udf(lambda z: anon_text(z),StringType())

# COMMAND ----------

unredacted_df = spark.sql("select * from default.phi_doctors_notes")

# COMMAND ----------

unredacted_df.select("comments").show(5,False)

# COMMAND ----------

redacted_df = unredacted_df.withColumn("redacted_comments", redact_textUDF(col("comments"))).withColumn("anonymized_comments", anon_textUDF(col("comments")))


# COMMAND ----------

display(redacted_df.select("comments","redacted_comments","anonymized_comments").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Other Items to implement
# MAGIC - Does a column contain PI, if so what entity types?
# MAGIC - Automated scan across all delta tables in a db to detect PI column by column, saving the results in another table for later action
# MAGIC - Implement feature that uniquely identifies entities in column and matches to row. For example, instead of ``[NAME_1]`` appearing in every row, ensure it is represented as ``[NAM_{1..N}]``

# COMMAND ----------


