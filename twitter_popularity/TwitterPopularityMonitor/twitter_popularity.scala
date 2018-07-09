import org.apache.log4j.{Level, Logger}

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf

import org.apache.spark.rdd.RDD

import org.apache.spark.sql._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Dataset

import twitter4j.Status
import twitter4j.User



object TwitterPopularityMonitor extends App {

    def remapHashtagsTweeter(status:Status) : Seq[(String, String)] = {
        val tweetUser = status.getUser.getScreenName()
        val hashtags = status.getText.split(" ").filter(_.startsWith("#"))

        return hashtags.map(hashtag => (hashtag, tweetUser)).toSeq
    }

    case class Tweet(hashTweet: Array[String], user: twitter4j.User, refs: Array[String])
        
    def remapHashtagsTweetRefs(rdd:RDD[twitter4j.Status]) : Dataset[Tweet] = {
	// Get the singleton instance of SparkSession
	val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate() 
	import spark.implicits._

	// Convert RDDs of the words DStream to DataFrame and run SQL query
    	rdd.foreach{case (status:Status) =>
     
      		// Convert RDD[twitter4j.Status] to RDD[case class]
      		val tweets = rdd.map(status => Record(status))

      		//val tweetsDataFrame = rdd.map(status => Tweet(status.getText.split(" ").filter(_.startsWith("#")),
		//					status.getUser(),
		//				status.getText.split(" ").filter(_.startsWith("@"))).toDF())

      		// Creates a temporary view using the DataFrame
		val tweetsDataFrame = tweets.toDF
      		tweetsDataFrame.createOrReplaceTempView("tweets")

	}

	// Aggregate tweet data in ttable using SQL and return it
      	return spark.sql("select hashtag, user, refs from tweets group by hashtag")
    }

    def Record(status:Status) : RDD[Tweet] = {
	val user = status.getUser()
	val hashtags = status.getText.split(" ").filter(_.startsWith("#"))
	val tweetRefs = status.getText.split(" ").filter(_.startsWith("@"))
	hashtags.map{hashtag => (hashtag, user, tweetRefs)};
    }
        
    if (args.length < 4) {
      System.err.println("Usage: TwitterPopularityMonitor <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    // Set logging level if log4j not configured (override by adding log4j.properties to classpath)
    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }

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

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None, filters)

    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
    val hashtagsTweetRefs = stream.transform(status => remapHashtagsTweetRefs(status))

    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
                     .map{case (topic, count) => (count, topic)}
                     .transform(_.sortByKey(false))
		     .map{case (count, topic) => (topic, count)}
		     .join(hashtagsTweetRefs)
		     .map{case (topic, (count, tweetRefs)) => (count, (topic, tweetRefs))}


    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
                     .map{case (topic, count) => (count, topic)}
                     .transform(_.sortByKey(false))
		     .map{case (count, topic) => (topic, count)}
		     .join(hashtagsTweetRefs)
		     .map{case (topic, (count, tweetRefs)) => (count, (topic, tweetRefs))}


    // Print popular hashtags
    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)

      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, hashtag, tweeters, tweetrefs) => { println("%s (%s tweets), tweeters: %s, tweetrefs: %s".format(hashtag, count, tweeters, tweetrefs))}}
    })

    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    ssc.start()
    ssc.awaitTermination()

}
