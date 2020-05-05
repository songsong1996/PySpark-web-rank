from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


spct = SparkContext.getOrCreate()
## create a spark session
spark_session = SparkSession(spct)
## create a spark stream to emitte files
stmct = StreamingContext(spct, 1)

## load p2t1.csv as file stream
whole_file = stmct.textFileStream('hdfs:/p2t1.csv')
# map file to content that we want 
rank_select = whole_file.map(lambda row: row.split('\t')[1])
## build a function for rank_select that select rank>0.5
rank_select1 = rank_select.filter(lambda r:float(r)>0.5)
# get count that we want 
counts = rank_select1.count()
counts.pprint()

## start stream
stmct.start() 
stmct.awaitTermination() 
stmct.stop() 
