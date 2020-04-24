from pyspark import SQLContext
from pyspark.sql import SparkSession
from graphframes import GraphFrame
from pyspark.sql import functions as F
from graphframes.lib import AggregateMessages as AM

import os
os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages graphframes:graphframes:0.3.0-spark2.0-s_2.11 pyspark-shell"

class Graphs(object):
    """Example GraphFrames for testing the API
    :param sql_context: spark sqlContext
    """

    def __init__(self, sql_context):
        self._sqlContext = sql_context

    def generate_example_graph(self):
        """A GraphFrame of friends in a (fake) social network."""
        # Vertex DataFrame
        v = self._sqlContext.createDataFrame(
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

        # Edge DataFrame
        e = self._sqlContext.createDataFrame(
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

        # Create a GraphFrame
        return v, e

def main():
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    sql_context = SQLContext(sc)

    v, e = Graphs(sql_context).generate_example_graph()
    g = GraphFrame(v,e)


if __name__ == "__main__":
    main()