# start a new SparkSession
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate() 

# create an RDD
tiny_rdd = spark.sparkContext.parallelize([1,2,3,4,5])

# transform tiny_rdd
transformed_tiny_rdd = tiny_rdd.map(lambda x: x+1) # apply x+1 to all RDD elements

# view the transformed RDD
transformed_tiny_rdd.collect()
# output:
# [2, 3, 4, 5, 6]

# execute action
print(rdd.count())

# output:
# 5

# we can run collect() on a small RDD
rdd.collect()
# output: [1, 2, 3, 4, 5]

rdd.take(2)
# output: [1, 2]

# add all elements together
print(rdd.reduce(lambda x,y: x+y))
# output: 15

# multiply all elements together
print(rdd.reduce(lambda x,y: x*y))
# output: 120

# start the accumlator at zero
counter = spark.sparkContext.accumulator(0)

# add 1 to the accumulator for each element
rdd.foreach(lambda x: counter.add(1))

print(counter)
# output: 5

# create an RDD
rdd = spark.sparkContext.parallelize(["Plane", "Plane", "Boat", "Car", "Car", "Boat", "Plane"])

# dictionary to broadcast
travel = {"Plane":"Air", "Boat":"Sea", "Car":"Ground"}

# create broadcast variable
broadcast_travel = spark.sparkContext.broadcast(travel)

# map the broadcast variable to the RDD
result = rdd.map(lambda x: broadcast_travel.value[x])

# view first four results
result.take(4)
# output : ['Air', 'Air', 'Sea', 'Ground']


# Create a new SparkSession
spark = SparkSession\
    .builder\
    .config('spark.app.name', 'learning_spark_sql')\
    .getOrCreate()

# Read in Wikipedia Unique Visitors Dataset
wiki_uniq_df = spark.read\
    .option('header', True) \
    .option('delimiter', ',') \
    .option('inferSchema', True) \
    .csv("wiki_uniq_march_2022_w_site_type.csv")

hrly_views_df\
    .filter(hrly_views_df.language_code == "kw.m")\
    .select(['language_code', 'article_title', 'hourly_count'])\
    .orderBy('hourly_count', ascending=False)\    
    .show(5, truncate=False)

query = """SELECT language_code, article_title, hourly_count
    FROM hourly_counts
    WHERE language_code = 'kw.m'
    ORDER BY hourly_count DESC"""

spark.sql(query).show(truncate=False)


ar_site_visitors = wiki_uniq_df\
                    .filter(wiki_uniq_df.language_code == 'ar')

ar_visitors_slim = wiki_uniq_df\
                    .filter(wiki_uniq_df.language_code == 'ar')\
                    .select(['domain','uniq_human_visitors'])


# show the DataFrame
ar_visitors_slim.show()

top_visitors_site_type = wiki_uniq_df\
                    .select(['site_type','uniq_human_visitors'])\
                    .groupBy('site_type')\
                    .sum()\
                    .orderBy('sum(uniq_human_visitors)', ascending=False)


# show the DataFrame
top_visitors_site_type.show()
