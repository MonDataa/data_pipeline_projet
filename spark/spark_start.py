from pyspark.sql import SparkSession
from pyspark import SparkContext,SparkConf
from pyspark.sql.functions import expr
import pyspark.sql.functions as F
import pyspark.sql.types as T

conf = SparkConf().setAppName("conf pro spark").setMaster("local")
sc=SparkContext.getOrCreate(conf)

spark = SparkSession.builder.appName("Python airflow d2").config("spark.some.config.option","some-value").getOrCreate()

data = spark.read.option('header','true').option('inferSchema','true').csv('/home/momo/Bureau/nifi_transformer/datav1.csv')