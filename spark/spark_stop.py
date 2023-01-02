from pyspark.sql import SparkSession
from pyspark import SparkContext,SparkConf
from pyspark.sql.functions import expr
import pyspark.sql.functions as F
import pyspark.sql.types as T

conf = SparkConf().setAppName("conf pro spark").setMaster("local")
sc=SparkContext.getOrCreate(conf)

sc.stop()