from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, regexp_extract, concat_ws, collect_list, sum as _sum,
    monotonically_increasing_id, udf
)
from pyspark.sql import Window
from pyspark.sql.types import StringType
import json
import re

spark = SparkSession.builder.appName("StructuredLogParser").getOrCreate()
df = spark.read.text("file:///app/data/**/*.gz")
timestamp_regex = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z"
df = df.withColumn("is_new_log", when(col("value").rlike(timestamp_regex), lit(1)).otherwise(lit(0)))
df = df.withColumn("row_id", monotonically_increasing_id())
w = Window.orderBy("row_id")
df = df.withColumn("log_id", _sum("is_new_log").over(w))
grouped = df.groupBy("log_id").agg(collect_list("value").alias("lines"))
grouped = grouped.withColumn("log_entry", concat_ws("\n", "lines"))
grouped = grouped.withColumn("timestamp", regexp_extract(col("log_entry"), timestamp_regex, 0))
grouped = grouped.withColumn("log_level", regexp_extract(col("log_entry"), r"\t(INFO|ERROR|DEBUG|WARN)\t", 1))

def extract_transaction_fields(entry):
    try:
        transaction = {}
        patterns = {
            "transaction_id": r"id:\s*'([^']+)'",
            "type": r"type:\s*'([^']+)'",
            "source": r"source:\s*'([^']+)'",
            "action": r"action:\s*'([^']+)'",
            "userId": r"userId:\s*'([^']+)'",
            "currency": r"currency:\s*'([^']+)'",
            "amount": r"amount:\s*([0-9.]+)",
            "vat": r"vat:\s*([0-9.]+)",
            "oldBalance": r"oldBalance:\s*([0-9.]+)",
            "newBalance": r"newBalance:\s*([0-9.]+)",
            "paymentBalance": r"paymentBalance:\s*([0-9.]+)"
        }
        for key, pattern in patterns.items():
            match = re.search(pattern, entry)
            transaction[key] = match.group(1) if match else None
        return json.dumps(transaction)
    except Exception:
        return None

extract_udf = udf(extract_transaction_fields, StringType())
grouped = grouped.withColumn("parsed_json", extract_udf(col("log_entry")))
parsed_df = grouped.filter(col("parsed_json").isNotNull())

def parse_json_to_columns(row):
    base = {
        "timestamp": row["timestamp"],
        "log_level": row["log_level"],
        "log_entry": row["log_entry"]
    }
    try:
        parsed = json.loads(row["parsed_json"])
        base.update(parsed)
    except:
        pass
    return base

parsed_rows = parsed_df.select("timestamp", "log_level", "log_entry", "parsed_json").collect()
structured_rows = [parse_json_to_columns(row.asDict()) for row in parsed_rows]

import pandas as pd
final_df = pd.DataFrame(structured_rows)
for colname in ["amount", "vat", "oldBalance", "newBalance", "paymentBalance"]:
    final_df[colname] = pd.to_numeric(final_df[colname], errors="coerce")
final_df["overdraft_flag"] = final_df["newBalance"] < 0
output_sdf = spark.createDataFrame(final_df)
output_sdf.coalesce(1).write.mode("overwrite").option("header", "true").csv("/app/output/parsed_logs.csv")
print("CSV file written to /app/output/parsed_logs.csv")
