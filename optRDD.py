from pyspark import SparkContext
sc=SparkContext("local", "count app")
words=sc.parallelize(
    ["scala", 
   "java", 
   "hadoop", 
   "spark", 
   "akka",
   "spark vs hadoop", 
   "pyspark",
   "pyspark and spark"]
)
# Operation count 
counts=words.count()
print("\nNumber of elements in RDD -> %i"%(counts))
# Return all element in RDD
coll = words.collect()
print("\nElement in RDD -> %s"%(coll))

# for element
def f(x):
    print(x)
fore = words.foreach(f)

# filter
words_filter = words.filter(lambda x: 'spark' in x)
filtered = words_filter.collect()
print("\nFilter RDD -> %s"%(filtered))

# Map
words_map=words.map(lambda x: (x,1))
mapping= words_map.collect()
print("\vKey values pair -> %s"% (mapping))

# Reduce
from operator import add
nums = sc.parallelize([1,2,3,4,5])
adding=nums.reduce(add)
print("\nAdding add the element -> %i"% (adding))

# Join
x = sc.parallelize([("spark", 1), ("hadoop", 4)])
y = sc.parallelize([("spark", 2), ("hadoop", 5)])
joined = x.join(y)
final = joined.collect()
print ("\nJoinc RDD -> %s" % (final))

# cache
words.cache()
caching = words.persist().is_cached
print("\nWords got cached -> %s"% (caching))

# broadcast var use to save copy data in all nodes
words_new = sc.broadcast(["scala", "java", "hadoop", "saprk", "akka"])
elem=words_new.value[2]
print("\nStored data -> %s"%(elem))

# Accumulator var use for agg info multiple workers
num = sc.accumulator(10)
def f(x):
    global num
    num+=x
rdd = sc.parallelize([20,30,40,50])
rdd.foreach(f)
final = num.value
print("\nAccumulator value í -> %i"% (final))