from pyspark.sql import SparkSession, SQLContext
import pyspark
from graphframes import GraphFrame
from graphframes.lib import AggregateMessages as AM

from pyspark.sql.functions import sum
from pyspark.sql.functions import desc, lit, col, when
from pyspark.sql.types import IntegerType


# # https://graphframes.github.io/graphframes/docs/_site/quick-start.html
spark = SparkSession.builder.appName("Python Spark graph example").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")
sqlContext = SQLContext(sc)

v = sqlContext.createDataFrame(
    [
        ("a", "Alice", 34),
        ("b", "Bob", 36),
        ("c", "Charlie", 30),
        ("d", "David", 29),
        ("e", "Esther", 32),
        ("f", "Fanny", 36),
        ("g", "Gabby", 60),
    ],
    ["id", "name", "age"],
)
# Edge Dataframe
e = sqlContext.createDataFrame(
    [
        ("a", "b", "friend"),
        ("b", "c", "follow"),
        ("c", "b", "follow"),
        ("f", "c", "follow"),
        ("e", "f", "follow"),
        ("e", "d", "friend"),
        ("d", "a", "friend"),
        ("a", "e", "friend"),
    ],
    ["src", "dst", "relationship"],
)
# create graph
g = GraphFrame(v, e)
g.vertices.show()
g.edges.show()

# Find the youngest user's age in the graph.
g.vertices.groupBy().min("age").show()

# Count the number of "follows" in the graph.
numFollows = g.edges.filter("relationship = 'follow'").count()

# motif finding
motifs = g.find("(a)-[e]->(b); (b)-[e2]->(c)").filter("a.id != c.id")

motifs = g.find("(a)-[e]->(b); (b)-[e2]->(a)")
motifs.show()
# More complex queries
motifs.filter("b.age > 30").show()


print("\ngenerate subgraph --- ")
g1 = (
    g.filterVertices("age > 30")
    .filterEdges("relationship = 'friend'")
    .dropIsolatedVertices()
)
g1.vertices.show()
g1.edges.show()

# Breadth-first search (BFS)
print("\n BFS")
paths = g.bfs(
    "name = 'Esther'",
    "age < 32",
    edgeFilter="relationship != 'friend'",
    maxPathLength=3,
).show()

# In-Degree and Out-Degree Metrics
print("\n Degree--------------")
inDeg = g.inDegrees
inDeg.orderBy(desc("inDegree"))
outDeg = g.outDegrees
outDeg.orderBy(desc("outDegree"))
degreeRatio = (
    inDeg.join(outDeg, "id")
    .selectExpr("*", "double(inDegree)/ double(outDegree) as degreeRatio")
    .orderBy(desc("degreeRatio"))
    .show(10, False)
)


# print("\n strong connected component")
# result = g.stronglyConnectedComponents(maxIter=10)
# result.select("id", "component").orderBy("component").show()

# Page rank
print("\n Page rank")
# # run until convergence to tol
# results = g.pageRank(resetProbability=0.15, tol=0.01)
# results.vertices.select("id", "pagerank").show()
# results.edges.select("src", "dst", "weight").show()

## Run PageRank personalized for vertex ["a", "b", "c", "d"] in parallel
# results4 = g.parallelPersonalizedPageRank(resetProbability=0.15, sourceIds=["a", "b", "c", "d"], maxIter=10)\


print("\n shortest paths")
results = g.shortestPaths(landmarks=["a", "d"])
results.select("id", "distances").show()

# # Saving and Loading GraphFrames
# g.vertices.write.parquet("hdfs://myLocation/vertices")
# g.edges.write.parquet("hdfs://myLocation/edges")
#
# # Load the vertices and edges back.
# sameV = sqlContext.read.parquet("hdfs://myLocation/vertices")
# sameE = sqlContext.read.parquet("hdfs://myLocation/edges")

# message passing via AggregateMessages
# For each user, sum the ages of the adjacent users.
msgToSrc = AM.dst["age"]
msgToDst = AM.src["age"]
agg = g.aggregateMessages(
    sum(AM.msg).alias("summedAges"), sendToSrc=msgToSrc, sentToDst=msgToDst
)
agg.show()
