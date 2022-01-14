from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession  # , functions as f
from graphframes import GraphFrame

conf = SparkConf().setAppName("MLBaconNumber") \
                  .setMaster("local[*]") \
                  .set("spark.jars", "/usr/lib/MLBaconNumber/graphframes-0.8.1-spark3.0-s_2.12.jar")
context = SparkContext(conf=conf)
spark = SparkSession(context)

v = spark.createDataFrame([
    ("a", "Alice", 34),
    ("b", "Bob", 36),
    ("c", "Charlie", 30),
    ("d", "David", 29),
    ("e", "Esther", 32),
    ("f", "Fanny", 36)
], ["id", "name", "age"])
# Edge DataFrame
e = spark.createDataFrame([
    ("a", "b", "friend"),
    ("b", "c", "follow"),
    ("c", "b", "follow"),
    ("f", "c", "follow"),
    ("e", "f", "follow"),
    ("e", "d", "friend"),
    ("d", "a", "friend")
], ["src", "dst", "relationship"])
# Create a GraphFrame
g = GraphFrame(v, e)
g.vertices.show()
