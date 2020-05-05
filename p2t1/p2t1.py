from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

## run a spark session to load file
schema_ = StructType().add("article", "string").add("ranking", "double")
spark_session = SparkSession.builder.getOrCreate()
df = spark_session \
    .readStream \
    .format('csv') \
    .option("sep",'\t') \
    .schema(schema_) \
    .load('hdfs:/p2t1.csv',header='true')

# select rank>0.5 and output it as a csv file
df_result=df.select(['article','ranking']).where('ranking>0.5')
q = df_result.writeStream() \
    .format('csv') \
    .option("path",'gs://assignment2_p2/p2t1_whole.csv') \
    .start()
# wait session until it finish
spark_session.streams.active
q.awaitTermination()
q.stop()

# start another spark session to read file and count
spark2 = SparkSession.builder.getOrCreate()
rank_df = spark2.read.csv('gs://assignment2_p2/p2t1_whole.csv',header=True)

print(rank_df.count())