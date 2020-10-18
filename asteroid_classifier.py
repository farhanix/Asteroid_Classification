# Enter main code here
import pandas as pd

from pyspark.sql import Row
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.types import FloatType
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql.functions import explode, col, udf, mean as _mean, stddev as _stddev, log, log10
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.functions import lit
import os

# import tensorflow as tf
# from tensorflow import feature_column
# from sklearn.model_selection import train_test_split

from pyspark.mllib.feature import StandardScaler
from pyspark.mllib.util import MLUtils

data_directory = 'E:\\University Of Toronto\\Courses\\MIE1628 - Big Data\\Projects\\Project_2_Asteroid'
test_data = os.path.join(data_directory, 'test_data.csv')
train_data = os.path.join(data_directory, 'train_data.csv')

sc = SparkContext.getOrCreate()
# spark = SparkSession.builder.appName('Asteroid Classifier').config().getOrCreate()
spark = SparkSession(sc)

print(sc.getConf().getAll())

df_train = spark.read.csv(train_data,header=True, inferSchema=True)
df_test = spark.read.csv(test_data,header=True, inferSchema=True)

def pre_process(df):
    
    df = df.withColumn('y', F.when(col('pha')=='N',0).otherwise(1))
    
    
    return df


df_train = pre_process(df_train)
df_train.columns
df_train.select('y').show()
df_train.select('y').distinct().show()

y_train = df_train.rdd.map(lambda x: x.y).collect()
y_train = df_train.select(col('y')).collect()


# df.registerTempTable('test_table')

# query_df = spark.sql('''
#                      select *
#                      from test_table                   
#                      ''')
                     
# query_df