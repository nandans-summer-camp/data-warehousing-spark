## Intro to Spark / Mapping / Reducing Tutorial

The tutorial is in the notebook `mapreduce.ipynb`.

To run this tutorial, run the Docker container: `jupyter/pyspark-notebook`!

## Spark Assignment

To complete the assignment you will need to access a 160GB dataset of tweets, which is available in the Google Cloud Storage bucket:

`gs://bgse-datawarehousing-random-tweets`

This bucket is public. You can view it in the browser through this link:

https://console.cloud.google.com/storage/browser/bgse-datawarehousing-random-tweets

You will see that it consists of a bunch of files named with a timestamp. Each of these files is a line-delimited json file (also known as json lines format), each JSON represents a tweet object from the Twitter API. You can read more about the format on [the Twitter site](https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/intro-to-tweet-json).

Google has written a "connector" that allows Spark to read directly from Google Cloud Storage buckets as though they were hdfs file paths. You can read about the connector here: https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage.

We want to analyze hashtags by looking at their cooccurence with other hashtgs. The final object we will want is:

* A scipy sparse matrix, dimensions NxN, where N is the number of hashtags analyzed. The values of the matrix should be the number of times that hashtags i,j coccur in the same tweet (thus it will be a symmetric matrix).

* A method for mapping hashtags to indices in the sparse matrix (and vice versa)

To do that, the first step will be to get out the hashtags from the tweet object. I recommend reading the data and using `.printSchema` to take a look at the JSON schema in Spark. We can ignore the "extended tweets" and instead just look at the basic tweet object, where the hashtags live under `entities.hashtags`.

I recommend as a first step, try to create an RDD where each element is a 2-tuple, with two tags that appeared together, like: `[('maga', 'trumptrain'),...,]`.

From there, you should try and create a data structure from which it is easy to create a scipy sparse matrix, and then aggregate all the information into a matrix. Think about what you want to do locally on the worker nodes and what you want to do on the driver.

NOTE: This will be a relatively long-running operation! 160GB is not a lot, but it is not so little either. I recommend trying to get everything working on a SINGLE json file and THEN trying to run it on the entire folder.

### Running Jupyter and Spark on Google Cloud

1. Make sure to redeem your $50 student credits!
2. Just follow the tutorial here: https://cloud.google.com/dataproc/docs/tutorials/jupyter-notebook

I recommend the following setup:

``` shell
gcloud beta dataproc clusters create dw-cluster \
--optional-components=ANACONDA,JUPYTER \
--image-version=1.4 \
--enable-component-gateway \
--bucket [THE BUCKET YOU CREATED FOR NOTEBOOK FILES] \
--project [YOUR PROJECT] \
--zone europe-west1-b \
--worker-machine-type n1-standard-4 \
--num-workers=2 \
--num-preemptible-workers=2
```

### Hints and methods to look at

0. Using the `samplingRatio=0.01` option with `read` in order to cut down on initial read time (look at documentation!)
1. Mapping and filtering to remove excess tweet data as early as possible.
2. `.cache()` might be helpful!
3. `.flatMap()` and `from itertools import permutations` will also be helpful.
4. `.reduceByKey()` will probably be needed and maybe even `.aggregate()` (although not necessary).
5. Look at the scipy sparse matrix types! Think about how you want to construct it. Try it out to make sure you understand how it works before trying to use it!
