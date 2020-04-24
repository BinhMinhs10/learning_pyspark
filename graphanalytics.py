#  $SPARK_HOME/bin/spark-submit --packages graphframes:graphframes:0.7.0-spark2.4-s_2.11 graphanalytics.py

# import os
# os.environ["PYSPARK_SUBMIT_ARGS"] = (
#         "--packages graphframes:graphframes:0.7.0-spark2.4-s_2.11 pyspark-shell"
# )

from pyspark.sql import SparkSession
import pyspark
from graphframes import GraphFrame
from pyspark.sql.functions import desc

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example")\
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
spark.sparkContext.setCheckpointDir("~/tmp/checkpoints")

bikeStations = spark.read.option("header", "true") \
    .csv("data/bike-data/201508_station_data.csv")
# print(bikeStations.collect())

tripData = spark.read.option("header", "true") \
    .csv("data/bike-data/201508_trip_data.csv")
tripData.show(5)

stationVertices = bikeStations.withColumnRenamed("name", "id").distinct()
tripEdges = tripData\
    .withColumnRenamed("Start Station", "src") \
    .withColumnRenamed("End Station", "dst")

stationGraph = GraphFrame(stationVertices, tripEdges)
stationGraph.cache()

print("\nTotal Number of Station: " + str(stationGraph.vertices.count()))
print("\nTotal Number of Trips in Graph: " + str(stationGraph.edges.count()))
print("\nTotal Number of Trips in Original Data: "+ str(tripData.count()))

# Query in graph
# stationGraph.edges.groupBy("src", "dst").count()\
    # .orderBy(desc("count")).show()

stationGraph.edges \
    .where("src = 'Townsend at 7th' OR dst = 'Townsend at 7th'") \
    .groupBy("src", "dst").count() \
    .show(10)

# # sub graph
# townAnd7thEdges = stationGraph.edges.where("src = 'Townsend at 7th' OR dst = 'Townsend at 7th'")
# subgraph = GraphFrame(stationGraph.vertices, townAnd7thEdges)

# # motif finding "triangle" pattern
# print("\nfind motif----------------")
# motifs = stationGraph.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[ca]->(a)")
# motifs.show()

# In-Degree and Out-Degree Metrics
inDeg = stationGraph.inDegrees
inDeg.orderBy(desc("inDegree")).show(5, False)

# Connected component
minGraph = GraphFrame(stationVertices, tripEdges.sample(False, 0.1))
cc = minGraph.connectedComponents()
cc.where("component != 0").show()



print("\nDONE======================\n")

