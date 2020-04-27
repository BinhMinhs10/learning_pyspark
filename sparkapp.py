from pyspark import SparkConf, SparkContext
import pyspark
from pyspark.serializers import MarshalSerializer

conf = SparkConf().setAppName("PySpark App").setMaster("local")
sc = SparkContext(conf=conf, serializer=MarshalSerializer())
sc.setLogLevel("WARN")

print(sc.parallelize(list(range(10000))).map(lambda x: 2 * x).take(10))
sc.stop()
