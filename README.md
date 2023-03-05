# UoM_MapReduce-vs-Spark

## MapReduce

1. Connect to EMR Cluster though SSH

![img.png](MapReduce/EMR_Cluster_Hive.PNG)

2. Create Table 

```
CREATE EXTERNAL TABLE IF NOT EXISTS flight_data(
  Counter int,
  Year int, 
  Month int,
  DayofMonth int,
  DayOfWeek int,
  DepTime int,
  CRSDepTime int,
  ArrTime int,
  CRSArrTime int,
  UniqueCarrier string,
  FlightNum int,
  TailNum string,
  ActualElapsedTime int,
  CRSElapsedTime int,
  AirTime int,
  ArrDelay int,
  DepDelay int,
  Origin string,
  Dest string,
  Distance int,
  TaxiIn int,
  TaxiOut int,
  Cancelled string,
  CancellationCode string,
  Diverted string,
  CarrierDelay int,
  WeatherDelay int,
  NASDelay int,
  SecurityDelay int,
  LateAircraftDelay int
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LOCATION '/user/tables/flight_data';
```

3. Load Data 

```
LOAD DATA INPATH '<s3 bucket link>' OVERWRITE INTO TABLE delayedFlights;
```

Now the data loaded to table, so we can run quaries


#### The airlines market has been faced losses due to the flight delay and there are many reasons for delaying a flight. In this Analysis, we are analyzing the various delay happens in airlines per year and run the queries as follows.

1. Year wise carrier delay from 2003-2010

```
SELECT Year As Year, avg((CarrierDelay /ArrDelay)*100) As AvgCarrierDelay
FROM flight_data
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year
ORDER BY Year;
```
![img.png](MapReduce/AvgCarrierDelay.PNG)


2. Year wise NAS delay from 2003-2010
```
SELECT Year As Year, avg((NASDelay /ArrDelay)*100) As AvgNASDelay
FROM flight_data
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year
ORDER BY Year;
```
![img.png](MapReduce/AvgNASDelay.PNG)


3. Year wise Weather delay from 2003-2010
```
SELECT Year As Year, avg((WeatherDelay /ArrDelay)*100) As AvgWeatherDelay
FROM flight_data
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year
ORDER BY Year;
```
![img.png](MapReduce/AvgWeatherDelay.PNG)


4. Year wise late aircraft delay from 2003-2010
```
SELECT Year As Year, avg((LateAircraftDelay /ArrDelay)*100) As AvgLateAircraftDelay
FROM flight_data
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year
ORDER BY Year;
```
![img.png](MapReduce/AvgLateAircraftDelay.PNG)


5. Year wise security delay from 2003-2010
```
SELECT Year As Year, avg((SecurityDelay /ArrDelay)*100) As AvgSecurityDelay
FROM flight_data
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year
ORDER BY Year;
```
![img.png](MapReduce/AvgSecurityDelay.PNG)



## Spark

1. Connect to EMR Cluster though SSH
![img.png](Spark/EMR_Cluster_Spark.PNG)

2. Open Pyshark Shell
```
pyspark
```

3. Import Libraries
```
from pyspark.sql import SparkSession
import time
```

4. Open a session 
```
spark = SparkSession.builder.appName("FlightData").getOrCreate()
```

5. Load file 
```
flight_data = spark.read.format("csv").option("header", "true").load("<s3 bucket link>")
flight_data.createOrReplaceTempView("flight_data")
```


Now the data loaded to table, so we can run quaries

#### The airlines market has been faced losses due to the flight delay and there are many reasons for delaying a flight. In this Analysis, we are analyzing the various delay happens in airlines per year and run the queries as follows.

1. Year wise carrier delay from 2003-2010

```
start_time = time.time()

spark.sql("""
	SELECT Year As Year, avg((CarrierDelay /ArrDelay)*100) As AvgCarrierDelay
	FROM flight_data
	WHERE Year >= 2003 AND Year <= 2010
	GROUP BY Year
	ORDER BY Year""").show()

end_time = time.time()
run_time = end_time - start_time
print("Query run time: ", run_time, " seconds")
```
![img.png](Spark/AvgCarrierDelay.PNG)


2. Year wise NAS delay from 2003-2010
```
start_time = time.time()

spark.sql("""
	SELECT Year As Year, avg((NASDelay /ArrDelay)*100) As AvgNASDelay
	FROM flight_data
	WHERE Year >= 2003 AND Year <= 2010
	GROUP BY Year
	ORDER BY Year""").show()

end_time = time.time()
run_time = end_time - start_time
print("Query run time: ", run_time, " seconds")
```
![img.png](Spark/AvgNASDelay.PNG)


3. Year wise Weather delay from 2003-2010
```
start_time = time.time()

spark.sql("""
	SELECT Year As Year, avg((WeatherDelay /ArrDelay)*100) As AvgWeatherDelay
	FROM flight_data
	WHERE Year >= 2003 AND Year <= 2010
	GROUP BY Year
	ORDER BY Year""").show()

end_time = time.time()
run_time = end_time - start_time
print("Query run time: ", run_time, " seconds")
```
![img.png](Spark/AvgWeatherDelay.PNG)


4. Year wise late aircraft delay from 2003-2010
```
start_time = time.time()

spark.sql("""
	SELECT Year As Year, avg((LateAircraftDelay /ArrDelay)*100) As AvgLateAircraftDelay
	FROM flight_data
	WHERE Year >= 2003 AND Year <= 2010
	GROUP BY Year
	ORDER BY Year""").show()

end_time = time.time()
run_time = end_time - start_time
print("Query run time: ", run_time, " seconds")
```
![img.png](Spark/AvgLateAircraftDelay.PNG)


5. Year wise security delay from 2003-2010
```
start_time = time.time()

spark.sql("""
	SELECT Year As Year, avg((SecurityDelay /ArrDelay)*100) As AvgSecurityDelay
	FROM flight_data
	WHERE Year >= 2003 AND Year <= 2010
	GROUP BY Year
	ORDER BY Year""").show()

end_time = time.time()
run_time = end_time - start_time
print("Query run time: ", run_time, " seconds")
```
![img.png](Spark/AvgSecurityDelay.PNG)


