# Intro to Spark

This module consists of the following:

1. A tutorial on using `map` and `reduce` and an introduction to laziness using python (`mapreduce.ipynb`).
2. A tutorial on Spark (`spark.ipynb`).
3. A bonus (mini) tutorial on Parquet and Spark SQL (`parquet-and-sql.ipynb`)
4. An assignment (below).

To run the tutorials, run the Docker container: `jupyter/pyspark-notebook`. It should work just like your other jupyter docker containers.

## Assignment

We are looking at tweets!

### Running Jupyter

To complete this assignment, you will need Spark and I recommend running it via Docker with the `jupyter/pyspark-notebook` image. Here are some extra options that might help:

1. `--memory=1g --memory-swap=1g ` - We'd like to learn about working with big data, but for logistical purposes, I gave you a small dataset. "Big" is a relative term, so we can simulate an environment of working with big data by restricting the memory of our Docker container with access to only 1GB of ram.

2. `-p 4040:4040 -p 8888:8888` - if you add port 4040, you will be able to access the Spark Web UI, which can be helpful to see how the "jobs" are created and assigned and see where it is in the process (gives you a "progress bar" of sorts, if nothing else!)


### Reading the Data

The data is separate from this repository, in the Google Classroom assignment, a file called `tweet-data.tar.gz`. You will need to uncompress and unarchive the data (~2GB uncompressed) into a folder and read the data into spark like so:

``` python
spark.options(samplingRatio=0.01).read.json('path/to/your/folder/')
```

The `.options(samplingRatio=0.01)` is something we haven't seen before: this just tells Spark "you're going to infer the schema to make your dataframe, but only scan 1% of the data to do that because I know it's pretty homogenous".

You can also add `.limit(N)` at the end to start with a smaller subset (`N`) of the data to play with. This is highly recommended!

Note that the folder will consist of many files, each file is a json lines file. Spark will automatically read them all together into one DataFrame (it's built to work with many files!). We aren't working with much data in this exercise, but you should still work within the "partitioned data" framework that would allow the same code to scale to a much bigger dataset. In other words: keep your data within the partitions until you aggregate it together.

You can see how many partitions your data has with the RDD method `.getNumPartitions()`. By looking at the Spark UI you should see how it creates a "task" for each partition and works through them both in parallel and serially!

### Your task

We want to analyze hashtags by looking at their cooccurence with other hashtgs. The final objects we will want is:

* A scipy sparse matrix, dimensions NxN, where N is the number of hashtags analyzed. The values of the matrix should be the number of times that hashtags i,j coccur in the same tweet (thus it will be a symmetric matrix). **Only count hashtag pairs that co-occur at least 3 times.**

* A data structure that allows us to map hashtags to indices in the sparse matrix (and vice versa)

And you will then use those objects to answer the following question:

1. How many times do #NewGreenDeal and #Brexit co-occur?
2. How many times do #Memes and #Merkel co-occur?
3. How many distinct hashtags are in your set of pairs that co-occur at least 3 times?

To do that, the first step will be to get out the hashtags from the tweet object. I recommend reading the data and using `.printSchema` to take a look at the JSON schema in Spark. We can ignore the "extended tweets" and instead just look at the basic tweet object, where the hashtags live under `entities.hashtags`.

This is a complex assignment and you will need to plan well in order to succeed! Break down the problem starting from the end and moving backwards: you know you need a Sparse matrix with the counts of all occurences summed over all the data in all the partitions, so what state could your data be in to make that easy to obtain?

Think about what you want to do locally within the partitions and then how you combine data across partitions. You should refer to [the documentation on PySpark RDDs](https://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD) heavily to see the available methods.

HINT: you sometimes need to aggregate multiple times (or do multiple passes over your data!)

HINT2: the `.aggregate` method might be useful to you.


### Hints for dealing with memory issues

If you're working with the Docker container restricted to `1g` of memory, you might blow up your memory if you are doing things in an inefficient way. Sometimes you get explicit out-of-memory errors, sometimes you get errors like "answer from Java side is empty" or "error occurred while trying to connect to the Java server", which means that your Spark Session died completely. In both cases, it's usually due to memory.

This should tell you that your Spark code isn't efficient enough!

Some tips:

1. Reduce the size of the data early, by selecting the parts you want from the Tweet.
2. Don't forget to only consider hashtags that co-occur at least 3 times. Be smart about when you filter for that.
3. Think about how to do as much as possible "within the partition" before aggregating across partitions.
4. Get things working first with a smaller set by using `.limit`.
