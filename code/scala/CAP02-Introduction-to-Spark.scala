// Simples task to create a range of numbers
val myRange = spark.range(1000).toDF("number")
myRange.show()

// Simple transformation to find all even numbers in DataFrame
val divisBy2 = myRange.where("number % 2 = 0")
divisBy2.show()

// The total numbers of records in the DataFrame
divisBy2.count()

/*
    An End-to-End Example
    Using Spark to analyze some flight data from United States Bureau of Transportation statistics
*/

import org.apache.spark.sql.SparkSession

// Inicializa o SparkSession
val spark = SparkSession.builder
    .appName("Flight Data Analysis")
    .config("spark.master", "local")
    .getOrCreate()

// Read CSV
val flightData2015 = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("/home/romulolvs/spark-guide/datasets/flight-data/csv/2015-summary.csv")

// Show the first 3 rows
flightData2015.show(3)

// Executing Explain Plan
flightData2015.sort("count").explain()

// Configuring the number of shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "5")

flightData2015.sort("count").take(2)

// Stop SparkSession
spark.stop()
