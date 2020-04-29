from pyspark.sql import SparkSession
from pyspark import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.serializers import MarshalSerializer

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

stringCSVRDD = sc.parallelize(
    [
        (123, "Katie", 19, "brown"),
        (234, "Michael", 22, "green"),
        (345, "Simone", 23, "blue"),
    ]
)
schema = StructType(
    [
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("age", LongType(), True),
        StructField("eyeColor", StringType(), True),
    ]
)

sql_context = SQLContext(sc)
swimmers = sql_context.createDataFrame(stringCSVRDD, schema)

# create temporary view
swimmers.createOrReplaceTempView("swimmers")
sql_context.sql("select name, eyeColor from swimmers where eyeColor like 'b%'").show()

import pandas as pd

df_pd = pd.DataFrame(
    data={
        "integers": [1, 2, 3],
        "float": [-1.0, 0.5, 2.7],
        "integers_arrays": [[1, 2], [3, 4, 5], [6, 7, 8, 9]],
    }
)
df = sql_context.createDataFrame(df_pd)
df.show()


def square(x):
    return x ** 2


# Return null if the output doesn't match data type
square_udf_int = F.udf(lambda z: square(z), IntegerType())
df.select(
    "integers",
    "float",
    square_udf_int("integers").alias("int_squared"),
    square_udf_int("float").alias("float_squared"),
).show()


def square_list(x):
    return [float(val) ** 2 for val in x]


square_list_udf = F.udf(lambda y: square_list(y), ArrayType(FloatType()))
df.select("integers_arrays", square_list_udf("integers_arrays")).show()
