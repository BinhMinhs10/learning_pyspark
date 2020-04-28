from pyspark import SQLContext
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F
from pyspark.sql.functions import monotonically_increasing_id

from graphframes import GraphFrame
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

    def generate_NBA_players(self):
        vertices = self._sqlContext.createDataFrame([('23', 'Michael', 'Jordan', 99),
                                                      ('33', 'Scottie', 'Pippen', 97),
                                                      ('1', 'Derrick', 'Rose', 94),
                                                      ('53', 'Artis', 'Gilmore', 94),
                                                      ('91', 'Dennis', 'Rodman', 93)],
                                                     ['id', 'firstname', 'lastname', 'rating'])

        edges = self._sqlContext.createDataFrame([('23', '33'),
                                       ('33', '23'),
                                       ('23', '91'),
                                       ('91', '23'),
                                       ('1', '33'),
                                       ('33', '1'),
                                       ('53', '23'),
                                       ('23', '53')],
                                      ['src', 'dst'])

        return vertices, edges

    def generate_example_dijstra(self):
        """A GraphFrame of friends in a (fake) social network."""
        # Vertex DataFrame
        v = self._sqlContext.createDataFrame(
            [
                ("a", "Alice", 34),
                ("b", "Bob", 36),
                ("c", "Charlie", 30),
                ("d", "David", 29),
                ("e", "Esther", 32),
                ("z", "Zalo", 36),
            ],
            ["id", "name", "age"],
        )

        # Edge DataFrame
        e = self._sqlContext.createDataFrame(
            [
                ("a", "b", 4),
                ("a", "c", 2),
                ("b", "c", 1),
                ("b", "d", 5),
                ("c", "d", 8),
                ("c", "e", 10),
                ("d", "e", 2),
                ("d", "z", 6),
                ("e", "z", 5)
            ],
            ["src", "dst", "score"],
        )

        # Create a GraphFrame
        return v, e

def min_rating_col(v, e, max_iterations=4):
    """
    :param v: vertices
    :param e: edges
    :param max_iterations: Iterative graph computations
    :return: print df
    """
    def new_rating(rating, id):
        return {"id": id, "rating": rating}
    player_type = types.StructType([
        types.StructField("id", types.StringType()),
        types.StructField("rating", types.IntegerType()),
    ])
    new_rating_udf = F.udf(new_rating, player_type)
    v = v.withColumn("minRating", new_rating_udf(v['rating'], v["id"]))
    cached_vertices = AM.getCachedDataFrame(v)

    g = GraphFrame(cached_vertices, e)
    g.vertices.show()
    g.edges.show()

    def min_rating(ratings):
        min_rating = -1
        min_rating_id = -1
        for rating in ratings:
            if min_rating == -1 or (rating.rating < min_rating):
                min_rating = rating.rating
                min_rating_id = rating.id
        return {"id": min_rating_id, "rating": min_rating}
    min_rating_udf = F.udf(min_rating, player_type)

    def compare_rating(old_rating, new_rating):
        return old_rating if old_rating.rating < new_rating.rating else new_rating

    compare_rating_udf = F.udf(compare_rating, player_type)

    # Iterative graph computations

    for _ in range(max_iterations):
        aggregates = g.aggregateMessages(F.collect_set(AM.msg).alias("agg"),
                                         sendToDst=AM.src["minRating"])
        res = aggregates.withColumn("newMinRating", min_rating_udf("agg")).drop("agg")
        new_vertices = g.vertices.join(res, on="id", how="left_outer")\
            .withColumnRenamed("minRating", "oldMinRating")\
            .withColumn("minRating", compare_rating_udf(F.col("oldMinRating"), F.col("newMinRating")))\
            .drop("oldMinRating").drop("newMinRating")
        cached_new_vertices = AM.getCachedDataFrame(new_vertices)
        g = GraphFrame(cached_new_vertices, g.edges)
        g.vertices.show()

def convert2undirect(g):

    # mirror = g.edges.withColumn('src_temp', F.col('src')) \
    mirror = g.edges.select(F.col("dst").alias("src"), F.col("src").alias("dst"))\
        .withColumn("_id", monotonically_increasing_id())

    cached_mirror = AM.getCachedDataFrame(mirror.join(
        g.edges.drop("src").drop("dst")
        .withColumn("_id", monotonically_increasing_id()), "_id", "outer").drop("_id"))

    g2 = GraphFrame(g.vertices, cached_mirror)
    cached_edges = AM.getCachedDataFrame(g.edges.union(g2.edges))

    g = GraphFrame(g.vertices, cached_edges)
    return g

def dijsktra(graph, initial, end, name_col_weight="score", directed=True):
    """

    :param graph: GraphFrames
    :param initial: id of node start
    :param end: id of node end
    :param name_col_weight:
    :param directed: boolean value
    :return: path shortest path
    if return -1 mean Route Not Possible
    """

    # check root and end node exit in shortest path

    shortest_paths = {str(initial): (None, 0)}
    current_node = str(initial)
    visited = set()

    while current_node != end:
        visited.add(current_node)

        if directed:
            # get all out node not in visited node
            destinations = graph.edges\
                .filter("src = '" + current_node + "'")\
                .filter(~graph.edges.dst.isin(visited))\
                .collect()
        else:
            # undirected graph so get all connect
            out_destinations = graph.edges\
                .filter("src = '" + current_node + "'") \
                .filter(~graph.edges.dst.isin(visited)) \
                .collect()
            in_destinations = graph.edges \
                .filter("dst = '" + current_node + "'") \
                .filter(~graph.edges.src.isin(visited)) \
                .withColumn("col_A_", F.col("dst")) \
                .withColumn("dst", F.col("src")) \
                .withColumn("src", F.col("col_A_")) \
                .drop("col_A_") \
                .collect()
            destinations = out_destinations + in_destinations

        weight_to_current_node = shortest_paths[current_node][1]

        for next_node in destinations:
            try:
                weight = next_node[name_col_weight] + weight_to_current_node
            except ValueError:
                return "Sorry, name_col_weight not exist in edges attribute"

            if next_node not in shortest_paths:
                shortest_paths[next_node["dst"]] = (current_node, weight)
            else:
                current_shortest_weight = shortest_paths[next_node["dst"]][1]
                if current_shortest_weight > weight:
                    shortest_paths[next_node["dst"]] = (current_node, weight)

        next_destinations = {node: shortest_paths[node] for node in shortest_paths if node not in visited}
        if not next_destinations:
            return -1
        # next node is destination with lowest weight
        current_node = min(next_destinations, key=lambda k: next_destinations[k][1])

    path = []
    while current_node is not None:

        path.append(current_node)
        next_node = shortest_paths[current_node][0]
        current_node = next_node
    # revert path
    path = path[::-1]
    return path


def main():
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    sql_context = SQLContext(sc)

    v, e = Graphs(sql_context).generate_NBA_players()
    # min_rating_col(v, e, 5)

    v, e = Graphs(sql_context).generate_example_dijstra()
    # Dijkstra Shortest Paths distance algorithm
    g = GraphFrame(v, e)

    print(dijsktra(g, "a", "g", directed=True))
    print(dijsktra(g, "a", "e", directed=False))

    # g_undirected = convert2undirect(g)
    # g_undirected.edges.show()
    # print(dijsktra(g_undirected, "a", "e", directed=False))


if __name__ == "__main__":
    main()
