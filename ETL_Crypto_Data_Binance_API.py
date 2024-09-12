# Databricks notebook source
spark.sql("CREATE SCHEMA IF NOT EXISTS dbo")
spark.sql("USE SCHEMA dbo")

# COMMAND ----------

import requests
import pandas as pd
import numpy as np
import time
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import from_unixtime, col, to_date, avg
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType

import os
import pickle
import numpy as np
from statsmodels.tsa.statespace.sarimax import SARIMAX

# COMMAND ----------

symbol = 'BTCUSDT'
interval = '1m'
schema = spark.sql("SELECT current_database()").collect()[0][0]

# COMMAND ----------

spark = SparkSession.builder.appName("CryptoData").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract Data by Binance API

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explanation of Binance API Data Collected
# MAGIC
# MAGIC The data collected from the Binance API consists of several fields representing various aspects of cryptocurrency trading data. Below is a description of the key fields:
# MAGIC
# MAGIC 1. **Open_time**:
# MAGIC    - **Description**: The timestamp when the candlestick opens, marking the beginning of the time interval.
# MAGIC    - **Data type**: Unix timestamp (in milliseconds).
# MAGIC
# MAGIC 2. **Open**:
# MAGIC    - **Description**: The asset price at the beginning of the candlestick interval (first price recorded within the time window).
# MAGIC    - **Data type**: Float.
# MAGIC
# MAGIC 3. **High**:
# MAGIC    - **Description**: The highest asset price during the candlestick interval.
# MAGIC    - **Data type**: Float.
# MAGIC
# MAGIC 4. **Low**:
# MAGIC    - **Description**: The lowest asset price during the candlestick interval.
# MAGIC    - **Data type**: Float.
# MAGIC
# MAGIC 5. **Close**:
# MAGIC    - **Description**: The asset price at the end of the candlestick interval (last price recorded before the window closes).
# MAGIC    - **Data type**: Float.
# MAGIC
# MAGIC 6. **Volume**:
# MAGIC    - **Description**: The total volume of the asset traded during the candlestick interval.
# MAGIC    - **Data type**: Float.
# MAGIC
# MAGIC 7. **Close_time**:
# MAGIC    - **Description**: The timestamp marking the end of the candlestick interval.
# MAGIC    - **Data type**: Unix timestamp (in milliseconds).
# MAGIC
# MAGIC 8. **Quote_asset_volume**:
# MAGIC    - **Description**: The total volume of trades in the quote asset (e.g., USD) during the candlestick interval.
# MAGIC    - **Data type**: Float.
# MAGIC
# MAGIC 9. **Number_of_trades**:
# MAGIC    - **Description**: The total number of trades executed during the candlestick interval.
# MAGIC    - **Data type**: Integer.
# MAGIC
# MAGIC 10. **Taker_buy_base_asset_volume**:
# MAGIC     - **Description**: The volume of the base asset (e.g., Bitcoin) bought by takers during the candlestick interval.
# MAGIC     - **Data type**: Float.
# MAGIC
# MAGIC 11. **Taker_buy_quote_asset_volume**:
# MAGIC     - **Description**: The volume of the quote asset (e.g., USD) used by takers to buy the base asset during the candlestick interval.
# MAGIC     - **Data type**: Float.
# MAGIC
# MAGIC 12. **Ignore**:
# MAGIC     - **Description**: Number of value being ignore
# MAGIC     - **Data type**: Float.
# MAGIC

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.appName("CryptoDataCollection").getOrCreate()

# COMMAND ----------

def collect_coin_data_binance(symbol, interval, start_time, end_time, limit=1000):
    url = 'https://api.binance.com/api/v3/klines'
    data = []

    while start_time < end_time:
        params = {
            'symbol': symbol,
            'interval': interval,
            'startTime': start_time,
            'endTime': end_time,
            'limit': limit
        }
        response = requests.get(url, params=params)
        response_data = response.json()

        if not response_data:
            break

        data.extend(response_data)
        start_time = response_data[-1][0] + 1
        time.sleep(0.1)

    columns = ["Open_time", "Open", "High", "Low", "Close", "Volume", "Close_time", "Quote_asset_volume", "Number_of_trades", "Taker_buy_base_volume", "Taker_buy_quote_volume", "Ignore"]
    df = spark.createDataFrame(data, columns)
    return df

# COMMAND ----------

def fetch_data(symbol, interval):
    end_time = int(datetime.now().timestamp() * 1000)
    table_name = f"{schema}.{symbol}_{interval}"
    
    if not spark.catalog._jcatalog.tableExists(table_name):
        start_time = int(datetime.strptime('2012-01-01', "%Y-%m-%d").timestamp() * 1000)
    else:
        df = spark.table(table_name)
        last_record = df.orderBy(col("Open_time").desc()).limit(1).collect()[0]
        last_open_time = last_record["Open_time"]
        
        next_open_time = last_open_time + timedelta(minutes=1)
        start_time = int(next_open_time.timestamp() * 1000)

    df_fetch = collect_coin_data_binance(symbol, interval, start_time, end_time)

    return df_fetch

# COMMAND ----------

df_fetched = fetch_data(symbol, interval)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Dataset

# COMMAND ----------

def transform_data(df):
    # Drop
    df = df.drop("Ignore", "Close_time")

    # Transform
    df = df.withColumn("Open_time", from_unixtime(col("Open_time") / 1000).cast("timestamp"))    
    float_columns = [col_name for col_name in df.columns if col_name not in ["Open_time", "Numer_of_trades"]]
    for col_name in float_columns:
        df = df.withColumn(col_name, col(col_name).cast(FloatType()))
    return df

# COMMAND ----------

df_fetched = transform_data(df_fetched)
df_fetched.printSchema()

# COMMAND ----------

display(df_fetched.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load to DBO

# COMMAND ----------

def load_dataframe(df, symbol,interval):
    if spark.catalog._jcatalog.tableExists(f"dbo.{symbol}_{interval}"):
        df_fetched.write.mode('append').saveAsTable(f"dbo.{symbol}_{interval}")
    else:
        df_fetched.write.saveAsTable(f"dbo.{symbol}_{interval}")

# COMMAND ----------

# spark.sql("DROP DATABASE IF EXISTS dbo CASCADE")
# spark.catalog.clearCache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Buiding model

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analytics Dataframe

# COMMAND ----------

df = spark.table(f'{schema}.{symbol}_{interval}')
display(df.limit(5))

# COMMAND ----------

df_grouped = df.withColumn('date', to_date(df['Open_time']))
df_grouped = df_grouped.groupBy('date').agg(avg('Close').alias('avg_close')).orderBy('date')

# COMMAND ----------

df_grouped = df_grouped.orderBy('date').toPandas()
plt.figure(figsize=(16, 8))  # Increase the figure size to fit more data

# Plot 'date' on the x-axis and 'avg_open' on the y-axis with points and lines
plt.plot(df_grouped['date'], df_grouped['avg_close'], marker='o', markersize=1, linestyle='-', linewidth=2, color='b')
# Add titles and labels
plt.title('Average Open Value Over Time')
plt.xlabel('Date')
plt.ylabel('Average Open Value ($)')

# Format the x-axis to display date in day-month-year format
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d-%m-%Y'))

# Set the locator to show ticks every 100 days
plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=100))

# Rotate x-axis labels for readability
plt.xticks(rotation=45)

# Enable grid
plt.grid(True)

# Format the y-axis to display values with a dollar sign
plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'${x:,.2f}'))

# Adjust layout and display the plot
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preprocessing Data

# COMMAND ----------

df_fetched = spark.table(f'{schema}.{symbol}_{interval}')
display(df_fetched.limit(5))

# COMMAND ----------

# Define the window specification
windowSpec = Window.orderBy("Open_time")

# Create a new column Future_Price which is the next Close value
df_prediction = df_fetched.withColumn(
"Close_lag", (F.lead("Close", 1).over(windowSpec))
)

# COMMAND ----------

display(df_prediction.limit(5))

# COMMAND ----------

df_prediction = df_prediction.drop('Open', 'Close', 'High', 'Low')

# COMMAND ----------

display(df_prediction)

# COMMAND ----------

# Convert Spark DataFrame to Pandas DataFrame for splitting
df_pd = df_prediction.toPandas()
df_pd['Open_time'] = df_pd['Open_time'].sort_index()
df_pd = df_pd.reset_index(drop=True).set_index('Open_time')

# COMMAND ----------

train_size = int(len(df_pd) - 20)
train_df, test_df = df_pd[:train_size], df_pd[train_size:]

train_endog = train_df["Close_lag"]
test_endog = test_df["Close_lag"]

# COMMAND ----------

df_pd

# COMMAND ----------

# MAGIC %md
# MAGIC ### SARIMA

# COMMAND ----------

# MAGIC %pip install river

# COMMAND ----------

# MAGIC %pip install cloudpickle

# COMMAND ----------

from river import time_series
from river import optim
from river import stream
from river import compose
from river import linear_model
from river import optim
from river import preprocessing
from river import datasets
import mlflow
import mlflow.sklearn

# COMMAND ----------

from pyspark.dbutils import DBUtils

# COMMAND ----------

def train_model(train_endog, model_filename='/dbfs/mnt/models/model.pkl'):
    dbutils = DBUtils(spark)
    files = dbutils.fs.ls('/mnt/models')

    if files or any(file.name == model_filename for file in files):
        print(f"Model {model_filename} found.")
        with open(model_filename, 'rb') as file:
            train_model = pickle.load(file)
    else:
        print(f'Create model')
        train_model =  (time_series.SNARIMAX(
                            p=1,
                            d=1,
                            q=1,
                            m=1440,
                            sp=1,
                            sq=1,
                            regressor=(
                                preprocessing.StandardScaler() |
                                linear_model.LinearRegression(
                                    intercept_init=110,
                                    optimizer=optim.SGD(0.01),
                                    intercept_lr=0.3
                                )
                            )
                        )
                    )
        
    
    print(f"Model: {train_model}")
    i = 0
    for y in train_endog:
        if i % 100000 == 0:
            print(f'{i}: {train_model.forecast(horizon=1)}, {y}')
        i+=1
        train_model.learn_one(y=y)
    
    with open(model_filename, 'wb') as file:
        pickle.dump(train_model, file)
    return train_model

# COMMAND ----------

train_model = train_model(train_endog=train_endog,  model_filename=f'/dbfs/mnt/models/{symbol}_Price_Prediction.pkl')

# COMMAND ----------

def prediction(model_filename, horizon):
    dbutils = DBUtils(spark)
    results = []
    files = dbutils.fs.ls('/mnt/models')
    if files or any(file.name == model_filename for file in files):
        print(f"Model {model_filename} found.")
        with open(model_filename, 'rb') as file:
            train_model = pickle.load(file)
        results = train_model.forecast(horizon=horizon)
        return results

# COMMAND ----------

prediction(f'/dbfs/mnt/models/{symbol}_Price_Prediction.pkl', 3)

# COMMAND ----------

# dbutils.fs.rm('/mnt/models/BTCUSDT_Price_Prediction.pkl')

# COMMAND ----------

forecast =  prediction(f'/dbfs/mnt/models/{symbol}_Price_Prediction.pkl', 20)

# COMMAND ----------

test_endog

# COMMAND ----------

forecast

# COMMAND ----------

# Assuming 'test_endog' contains the actual values and 'forecast' contains the predicted values
plt.figure(figsize=(12, 6))

# Plot actual values
plt.plot(test_endog.index, test_endog, label='Actual')

# Plot predicted values
plt.plot(test_endog.index, forecast, label='Predicted', linestyle='--')

plt.xlabel('Time')
plt.ylabel('Values')
plt.title('Actual vs Predicted Values')
plt.legend()
plt.grid(True)
plt.show()
