# ETL_Crypto_Data-Binance_API

## Description

This project implements an ETL (Extract, Transform, Load) pipeline for cryptocurrency data from the Binance API. The data is processed to support analysis and forecasting of the cryptocurrency market.

## Main Features

Main steps: Can be streaming by Apache Spark to collect DataFrame
- Extract: Fetch real-time data from Binance API.
- Transform: Clean and process the data, including converting time formats, handling missing data, and calculating key indicators.
- Load: Store the processed data into a database or storage system.

Bonus step: I initially thought it would be interesting to build a model to forecast BTC price and become wealthy, but after spending 3 days trying to figure out how to build a model that runs on streaming , I realized the not fun.
- Build SARIMA online-learning model (but it's so bad :D)

**Warning: This article is for educational purposes only. Please do not follow it!!!(I think you are crazy to follow terrify model like this repo)**

## Work-flow

Because this notebook is written on Databricks, the file type when in Github is Python:
### Extract:
By using Binance API to get Data. Also, since this is free, Binance cannot provide real-time data frame (about 8 hours delay)

![img](https://ibb.co/y0tqBj4)

##### Description:

The collect_coin_data_binance function retrieves historical candlestick (kline) data for a given cryptocurrency symbol from Binance's API within a specified time range. The data is collected in chunks due to API limits, and each response is added to a list. Once all the data is collected, it is converted into a Spark DataFrame for further processing.
Parameters:
- symbol (str): The trading pair symbol for the cryptocurrency (e.g., "BTCUSDT").
- interval (str): The interval between data points (e.g., "1m", "1h", "1d"). This represents the frequency of the candlestick data.
- start_time (int): The starting timestamp (in milliseconds) from which the data retrieval begins.
- end_time (int): The ending timestamp (in milliseconds) where data retrieval stops.
- limit (int, optional): The maximum number of records per API request (default is 1000).

Read more Binance API: ![here](https://binance-docs.github.io/apidocs/spot/en/)

![img](https://ibb.co/hfxLYJW)
##### Description:

The fetch_data function retrieves cryptocurrency market data (candlestick data) for a specified symbol and interval from Binance’s API. It ensures that new data is fetched by checking if the data already exists in the database (Spark table). If the data is present, the function fetches the data from the last known timestamp. Otherwise, it retrieves historical data starting from January 1, 2012. This is the main function for streaming extract.

### Transform

![img](https://ibb.co/5hzVP71)

##### Description:
The transform_data function processes and transforms a Spark DataFrame containing cryptocurrency candlestick data by applying column modifications. You can change what you want to transform your Dataframe to here.

### Load

![img](https://ibb.co/jrTStxH)

##### Description:
The load_dataframe function writes a Spark DataFrame into a database table.

**Which 3 steps above, you can schedule for Spark to collect dataframe for personal purposes**

### Analytics & Buiding model
Since I couldn't find any library or framework that can solve online time series (although I use ARIMAX and the result is really good but this can streaming), this step just stops at implementing the model using River framework (actually, I don't understand how river SNARIMAX actually works) and I decided not to explain this step.