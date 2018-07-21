import org.apache.log4j.{Level, Logger}

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf

import org.apache.spark.rdd.RDD

import org.apache.spark.sql._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import scala.collection.mutable.WrappedArray


import twitter4j.Status
import twitter4j.User



object TwitterPopularityMonitor extends App {

    // These two values are the configured windows over which tweets will be collected for display
    val LONGER_SECONDS = 120000
    val SHORTER_SECONDS = 5000


    // Utility function for string mapping - details with Array[Any] results that are problematic to handle
    def mapArray(initial:String, inputArray : Array[Any], prefix:String) : String = {
	val outputBuilder = new StringBuilder("")
        for (item<-inputArray) {
		outputBuilder.append(" ")
		outputBuilder.append(initial)
 		outputBuilder.append(prefix)
		outputBuilder.append(item)
	}

	return outputBuilder.toString()
    }


    // Get a list of users recorded as having tweeted a given hashtag, from within the tweets detail DataFrame recently collected
    def getUsers(hashtag:String) : String = {
	val userNamesResult = spark.sql("SELECT userName FROM hashtagDetail WHERE hashtag='%s'".format(hashtag)).rdd.map(r => r(0)).collect()
	return mapArray("tweeters: ", userNamesResult, "")
    }



    // Get a list of users referenced within tweets with a given hashtag, from within the tweets detail DataFrame recently collected
    def getRefs(hashtag:String) : String = {
	val refsResult = spark.sql("SELECT refs FROM hashtagDetail WHERE hashtag='%s'".format(hashtag)).rdd.map(r => r(0)).collect()
	return mapArray("tweet refs: ", refsResult, "")
    }


    // This case class will be used for mapping from a spark.streaming.DStream to a spark.sql.RDD, for collection in a DataFrame
    case class Tweet(hashTweet: Array[String], userName:String, refs: Array[String])


    // Translate an obeserved tweet into a <Tweet> object, with fields including hashtags, user and references
    def Record(status:Status) : Tweet = {
	val userObj = status.getUser()
	val userName = if ( userObj != null ) { userObj.getScreenName() } else { null }

	val tmpHashtags = status.getText.split(" ").filter(_.startsWith("#"))
	val hashtags = if (tmpHashtags.size > 0) { tmpHashtags } else { null }

	val tmpTweetRefs = status.getText.split(" ").filter(_.startsWith("@"))
	val tweetRefs = if (tmpTweetRefs.size > 0) { tmpTweetRefs } else { null }

	return Tweet(hashtags, userName, tweetRefs)
    }


    // **************************************
    // Here's where the main processing starts:
    // **************************************

    // Validate parameters, ensuring that all the Twitter credential keys are supplied at the command line       
    if (args.length < 4) {
      System.err.println("Usage: TwitterPopularityMonitor <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    // Set logging level if log4j not configured (override by adding log4j.properties to classpath)
    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }


    // Collect the Twitter credentials
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setAppName("TwitterPopularityMonitor")

    // check Spark configuration for master URL, set it to local if not configured
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    }


    // Setup the Spark Streaming Context for a Twitter Feed
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None, filters)



    // **************************************
    // Here's where the logic starts for mapping the tweet stream into collections and windows
    // **************************************

    // Create a map of hashtags in tweets
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    // Get the singleton instance of SparkSession
    val spark= SparkSession.builder().getOrCreate()
    import spark.implicits._

    // Map the stream of tweets into a stream of <Tweet> objects
    val hashtagsTweetRefs = stream.map(status => Record(status))


    // Iterate through the collected <Tweet> objects, creating spark.sql DataFrames to map these to collectable detail
    hashtagsTweetRefs.foreachRDD{ tweet =>
	// Get the singleton instance of SparkSession
	//val spark = SparkSession.builder.config(tweet.sparkContext.getConf).getOrCreate()
	//import spark.implicits._

	val tweetDF = tweet.toDF("hashtags", "userName", "refs")
	tweetDF.createOrReplaceTempView("tweets")
	val summaryDF = spark.sql("SELECT explode(hashtags) hashtag, userName, refs from tweets")
	summaryDF.createOrReplaceTempView("hashtagSummary")
	val detailDF = spark.sql("SELECT hashtag, userName, explode(refs) refs from hashtagSummary")
	detailDF.createOrReplaceTempView("hashtagDetail")
    }


    // This collects tweets with the longer configured window
    val topCountsLonger = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(LONGER_SECONDS))
                     .map{case (topic, count) => (count, topic)}
                     .transform(_.sortByKey(false))


    // This collects tweets with the shorter configured window
    val topCountsShorter = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(SHORTER_SECONDS))
                     .map{case (topic, count) => (count, topic)}
                     .transform(_.sortByKey(false))


    // Print popular hashtags
    topCountsLonger.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last %s milliseconds (%s total):".format(LONGER_SECONDS, rdd.count()))

      topList.foreach{case (count, hashtag) => { val tweeters=getUsers(hashtag); val tweetrefs=getRefs(hashtag); println("%s (%s tweets) %s %s".format(hashtag, count, tweeters, tweetrefs))}}
    })

    topCountsShorter.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last %s milliseconds (%s total):".format(SHORTER_SECONDS, rdd.count()))
      
      topList.foreach{case (count, hashtag) => { val tweeters=getUsers(hashtag); val tweetrefs=getRefs(hashtag); println("%s (%s tweets) %s %s".format(hashtag, count, tweeters, tweetrefs))}}
    })

    ssc.start()
    ssc.awaitTermination()

}
