from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import timedelta, datetime
import pandas as pd

####### Spark session
spark = SparkSession.builder \
    .appName("FraudDetection_PandasState") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


###### Kafka source
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "credit_card_transactions") \
    .option("startingOffsets", "latest") \
    .load()


####### Input schema
schema = StructType([
    StructField("trans_id", IntegerType()),
    StructField("card_no", StringType()),
    StructField("amount", StringType()),
    StructField("trans_status", StringType()),
    StructField("trans_time", StringType())
])

df = raw_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

df = df.withColumn(
    "trans_time",
    to_timestamp("trans_time", "yyyy-MM-dd HH:mm:ss")
)

events = df.select("card_no", "trans_status", "trans_time")

##### Output & state schemas

output_schema = StructType([
    StructField("card_no", StringType()),
    StructField("alert_type", StringType()),
    StructField("invalid_count", IntegerType()),
    StructField("alert_time", TimestampType())
])

####### State stored as tuple: (count, last_time_epoch)
state_schema = StructType([
    StructField("count", IntegerType()),
    StructField("last_time_epoch", LongType())
])

###### Stateful fraud detection logic (FINAL)

def detect_fraud(card_no, pdf_iter, state):

    #### Combine pandas batches
    pdf = pd.concat(list(pdf_iter))
    pdf["trans_time"] = pd.to_datetime(pdf["trans_time"])

    #### Load state (PROPERTY, not function)
    if state.exists:
        prev = state.get        # <-- NO ()
        count = int(prev[0])    # tuple index
        last_epoch = prev[1]

        if last_epoch is not None:
            last_time = datetime.fromtimestamp(last_epoch)
        else:
            last_time = None
    else:
        count = 0
        last_time = None

    alerts = []

    #### Sort by event time
    pdf = pdf.sort_values("trans_time")

    for _, row in pdf.iterrows():

        if row["trans_status"] != "Invalid":
            continue

        current_time = row["trans_time"].to_pydatetime()

        if last_time is None:
            count = 1
        else:
            if current_time - last_time <= timedelta(minutes=5):
                count += 1
            else:
                count = 1

        last_time = current_time

        #### Fraud condition
        if count == 3:
            alerts.append({
                "card_no": str(card_no),
                "alert_type": "3_CONSECUTIVE_INVALIDS",
                "invalid_count": 3,
                "alert_time": current_time
            })

            # Reset after alert
            count = 0
            last_time = None

    ### Update state as tuple
    state.update((
        count,
        int(last_time.timestamp()) if last_time else None
    ))

    out_df = pd.DataFrame(alerts)

    ### Enforce Arrow types
    if not out_df.empty:
        out_df["card_no"] = out_df["card_no"].astype(str)
        out_df["alert_type"] = out_df["alert_type"].astype(str)
        out_df["invalid_count"] = out_df["invalid_count"].astype("int64")
        out_df["alert_time"] = pd.to_datetime(out_df["alert_time"])

    ### Must return iterable
    return [out_df]

##### Apply pandas state

fraud_df = events.groupBy("card_no").applyInPandasWithState(
    func=detect_fraud,
    outputStructType=output_schema,
    stateStructType=state_schema,
    outputMode="append",
    timeoutConf="NoTimeout"
)

#### Write alerts to Kafka
query = fraud_df.selectExpr(
    "CAST(card_no AS STRING) AS key",
    "to_json(struct(*)) AS value"
).writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "fraud_alerts") \
    .option("checkpointLocation", "/tmp/fraud_pandas_state_checkpoint") \
    .trigger(processingTime="2 seconds") \
    .start()

query.awaitTermination()
