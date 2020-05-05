from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as pyf
import re

spark = SparkSession.builder.getOrCreate()
df = spark.read.format('xml').options(rowTag='page').load('hdfs:/enwiki_small.xml')
info = df.select('title', 'revision.text._VALUE')


# Define UDFs
def select_links(l):
    new_links = []
    for link in l:
        if '|' in link:
            link = link.split('|')[0]
        if '#' not in link:
            if link.startswith(u'Category:') or (':' not in link):
                new_links.append(link)
    if len(new_links) == 0:
        return None
    return new_links

spark.udf.register("select_links", select_links)


def take_lower(l):
    if l == None:
        return l
    else:
        return l.lower()

spark.udf.register("take_lower", take_lower)


# Convert string to lowercase
both_lower = info.rdd.map(lambda x: Row(**{'title': take_lower(x['title']), '_VALUE': take_lower(x['_VALUE'])}))
# Remove None
filter_out_none = both_lower.filter(lambda x: x['title'] != None and x['_VALUE'] != None)
# Find all links inside '[[]]'
step1 = filter_out_none.map(lambda x: Row(**{'title': x['title'], '_VALUE': re.findall("\[\[(.*?)\]\]", x['_VALUE'])}))
# Get the links that meeting the requirments
step2 = step1.map(lambda x: Row(**{'title': x['title'], '_VALUE': select_links(x['_VALUE'])}))
# Convert RDD to Dataframe
df_2 = step2.toDF()
# Explode the Dataframe
result = df_2.select(df_2.title, pyf.explode(df_2._VALUE))
# Order the Dataframe by 'title' and 'col' in ascending order
sorted_result = result.orderBy(['title', 'col'], ascending = True)


# When the input is enwiki_small.xml, output the first 5 rows of the result.
# final_output = spark.createDataFrame(sorted_result.take(5))
# final_output.repartition(1).write.format('com.databricks.spark.csv').option("delimiter", '\t').save('hdfs:/p1t2.csv', header = 'true')


# When the input is enwiki_test.xml or enwiki_whole.xml, output the whole result.
sorted_result.write.format('com.databricks.spark.csv').option("delimiter", '\t').save('hdfs:/p1t2.csv', header = 'true')