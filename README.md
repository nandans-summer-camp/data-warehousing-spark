# Intro to Spark

This module consists of the following:

1. A tutorial on using `map` and `reduce` and an introduction to laziness using python (`mapreduce.ipynb`).
2. A tutorial on Spark (`spark.ipynb`).
3. A bonus (mini) tutorial on Parquet and Spark SQL (`parquet-and-sql.ipynb`)
4. An assignment (below).

To run the tutorials, run the Docker container: `jupyter/pyspark-notebook`. It should work just like your other jupyter docker containers.

## Assignment

We are looking at tweets!

### Reading the Data

The data is separate from this repository, in the Google Classroom assignment, a file called `tweet-data.tar.gz`. You will need to uncompress and unarchive the data into a folder and read the data into spark like so:

``` python
spark.options(samplingRatio=0.01).read.json('path/to/your/folder/')
```

The `.options(samplingRatio=0.01)` is something we haven't seen before: this just tells Spark "you're going to infer the schema to make your dataframe, but only scan 1% of the data to do that because I know its pretty homogenous".

You can also add `.limit(N)` to start with a smaller subset (`N`) of the data to play with.

Note that the folder will consist of many files, each file is a json lines file. Spark will automatically read them all together into one DataFrame (it's built to work with many files!). The dataframe will have the same number of partitions as files, by default. We aren't working with much data in this exercise, but you should still work within the "partitioned data" framework that would allow the same code to scale to a much bigger dataset. In other words: keep your data within the partitions until you aggregate it together.

### Your task

We want to analyze hashtags by looking at their cooccurence with other hashtgs. The final objects we will want is:

* A scipy sparse matrix, dimensions NxN, where N is the number of hashtags analyzed. The values of the matrix should be the number of times that hashtags i,j coccur in the same tweet (thus it will be a symmetric matrix). **Only count hashtag pairs that co-occur at least 3 times.**

* A data structure that allows us to map hashtags to indices in the sparse matrix (and vice versa)

And you will then use those objects to answer the following questions:

* How many times do NewGreenDeal and Brexit co-occur?
* What tweet pair co-occurred most often?

To do that, the first step will be to get out the hashtags from the tweet object. I recommend reading the data and using `.printSchema` to take a look at the JSON schema in Spark. We can ignore the "extended tweets" and instead just look at the basic tweet object, where the hashtags live under `entities.hashtags`.

This is a complex assignment and you will need to plan well in order to succeed! Break down the problem starting from the end and moving backwards: you know you need a Sparse matrix with the counts of all occurences summed over all the data in all the partitions, so what state could your data be in to make that easy to obtain?

Think about what you want to do locally within the partitions and then how you combine data across partitions. You should refer to [the documentation on PySpark RDDs](https://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD) heavily to see the available methods.

HINT: you sometimes need to aggregate multiple times (or do multiple passes over your data!)

HINT2: the `.aggregate` method might be useful to you.
