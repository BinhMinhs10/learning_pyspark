from pyspark.sql import SparkSession
import pyspark
from graphframes import GraphFrame
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example")\
    .getOrCreate()


bikeStations = spark.read.option("header", "true") \
    .csv("data/bike-data/201508_station_data.csv")
# print(bikeStations.collect())

tripData = spark.read.option("header", "true") \
    .csv("data/bike-data/201508_trip_data.csv")

stationVertices = bikeStations.withColumnRenamed("name", "id").distinct()
tripEdges = tripData\
    .withColumnRenamed("Start Sattion", "src") \
    .withColumnRenamed("End Sattion", "dst")

stationGraph = GraphFrame(stationVertices, tripEdges)
stationGraph.cache()