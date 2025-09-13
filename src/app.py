import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, regexp_extract, concat_ws, collect_list, sum as _sum
from pyspark.sql import Window
from pyspark.sql.functions import monotonically_increasing_id, udf
from pyspark.sql.types import StringType
import pandas as pd
import re
import json

st.set_page_config(layout="wide")
st.title("Balance Sync Log Analyzer")

@st.cache_resource
def get_spark():
    return SparkSession.builder.appName("StreamlitPySparkApp").getOrCreate()

spark = get_spark()

@st.cache_data(ttl=600)
def load_and_parse_logs():
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

    def extract_fields(entry):
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

    extract_udf = udf(extract_fields, StringType())
    grouped = grouped.withColumn("parsed_json", extract_udf(col("log_entry")))
    parsed_df = grouped.select("timestamp", "log_level", "log_entry", "parsed_json").filter(col("parsed_json").isNotNull())
    pandas_df = parsed_df.toPandas()

    extracted_rows = []
    for _, row in pandas_df.iterrows():
        base = {
            "timestamp": row["timestamp"],
            "log_level": row["log_level"],
            "log_entry": row["log_entry"]
        }
        try:
            parsed = json.loads(row["parsed_json"])
            base.update(parsed)
            extracted_rows.append(base)
        except:
            pass

    final_df = pd.DataFrame(extracted_rows)
    for colname in ["amount", "vat", "oldBalance", "newBalance", "paymentBalance"]:
        final_df[colname] = pd.to_numeric(final_df[colname], errors="coerce")
    final_df["overdraft_flag"] = final_df["newBalance"] < 0
    return final_df

logs_df = load_and_parse_logs()

st.sidebar.subheader("🔍 Filters")
log_levels = logs_df["log_level"].dropna().unique().tolist()
selected_levels = st.sidebar.multiselect("Log Level", options=log_levels, default=log_levels)
users = logs_df["userId"].dropna().unique().tolist()
selected_user = st.sidebar.selectbox("Filter by User", options=["All"] + users)
only_overdrafts = st.sidebar.checkbox("⚠️ Show only Overdrafts", value=False)

filtered_df = logs_df[logs_df["log_level"].isin(selected_levels)]
if selected_user != "All":
    filtered_df = filtered_df[filtered_df["userId"] == selected_user]
if only_overdrafts:
    filtered_df = filtered_df[filtered_df["overdraft_flag"] == True]

st.subheader(f"Filtered Log Data ({len(filtered_df)} entries)")
st.dataframe(filtered_df, use_container_width=True)

st.download_button(
    label="Download CSV",
    data=filtered_df.to_csv(index=False),
    file_name="filtered_balance_logs.csv",
    mime="text/csv"
)
