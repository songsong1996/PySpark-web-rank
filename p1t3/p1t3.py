from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, when

spark = SparkSession.builder.getOrCreate()

# Define UDFs
def contribution(rank, n_neighbors):
    ''' 
    Calculate the contribution A giving to B.
    '''
    contribute = rank / n_neighbors
    return contribute

spark.udf.register("contribution", contribution)


def rank(total_contribution):
    '''
    Calculate the rank of B.
    '''
    rank = total_contribution * 0.85 + 0.15
    return rank

spark.udf.register("rank", rank)


articles = spark.read.format('csv').option("delimiter", '\t').load('hdfs:/p1t2_small.csv', header = 'true').withColumnRenamed('title', 'A').withColumnRenamed('col', 'B')
# Initialize the rank of article to be 1
ranks = articles.withColumn('Rank', lit(1))
# Count the number of neighbors of A
n_neighbors = ranks.groupBy('A').count().select(col('A').alias('A_'), 'count')
# Save as a csv file
ranks.write.format('com.databricks.spark.csv').option("delimiter", '\t').save('hdfs:/ranks_0.csv', header = 'true')


# Iterations
for i in range(10):
    # Load the file
    ranks_ = spark.read.format('csv').option("delimiter", '\t').load('hdfs:/ranks_' + str(i) + '.csv', header = 'true')
    # Join ranks with n_neighbors and then calculate the contribution each A giving to each B
    contribution_A_to_B = ranks_.join(n_neighbors, ranks_.A == n_neighbors.A_, 'left').select('A', 'B', 'count', 'Rank').withColumnRenamed('count', 'NeighborsNum_of_A').withColumn('Contribution_of_A_to_B', contribution(col('Rank'), col('NeighborsNum_of_A')))
    # Calculate the total contribution B receiving
    totle_contributions_to_B = contribution_A_to_B.groupBy('B').agg({'Contribution_of_A_to_B': 'sum'}).withColumnRenamed('sum(Contribution_of_A_to_B)', 'Total_Contribution')
    # Calculate the updated rank of B
    B_Rank = totle_contributions_to_B.withColumn('Updated_Rank', rank(col('Total_Contribution'))).withColumnRenamed('B', 'Article')
    # Join contribution_A_to_B with B_Rank on contribution_A_to_B.A == B_Rank.Article
    combine = contribution_A_to_B.join(B_Rank, contribution_A_to_B.A == B_Rank.Article, 'left')
    # If A has an updated rank, then update it. If it doesn't, then still use the old rank, which is 1.
    update_A_Rank = combine.withColumn('New_Rank', when(col('Updated_Rank').isNull(), col('Rank')).otherwise(col('Updated_Rank')))
    # Update ranks for next iteration
    ranks_ = update_A_Rank.select('A', 'B', 'New_Rank').withColumnRenamed('New_Rank', 'Rank')
    # Save as a csv file
    ranks_.write.format('com.databricks.spark.csv').option("delimiter", '\t').save('hdfs:/ranks_'+ str(i + 1) +'.csv', header = 'true')


# Save as a csv file
# combine: contains the article names in the right column (B) of the input file and the corresponding ranks
combine.write.format('com.databricks.spark.csv').option("delimiter", '\t').save('hdfs:/combine.csv', header = 'true')


# Load the csv file
combine_ = spark.read.format('csv').option("delimiter", '\t').load('hdfs:/combine.csv', header = 'true')


# If A has an updated rank, then update it. If it doesn't, then assign 0.15 to be its final rank.
updated_ranks = combine_.withColumn('Rank', when(col('Updated_Rank').isNull(), 0.15).otherwise(col('Updated_Rank'))).select('A', 'Rank').withColumnRenamed('A', 'Article').distinct()

# Sort the output by 'Article' and 'Rank' in ascending order.
output = updated_ranks.orderBy(updated_ranks.Article.asc())


# Save the first five rows as a .csv file
spark.createDataFrame(output.take(5)).write.format('com.databricks.spark.csv').option("delimiter", '\t').save('hdfs:/p1t3_small.csv', header = 'true')

# Save as a .csv file
# output.write.format('com.databricks.spark.csv').option("delimiter", '\t').save('hdfs:/p1t3.csv', header = 'true')