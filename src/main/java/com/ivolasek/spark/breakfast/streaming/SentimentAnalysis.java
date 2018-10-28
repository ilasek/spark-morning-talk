package com.ivolasek.spark.breakfast.streaming;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Date;
import java.util.Properties;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.ConfigurationBuilder;

/**
 * <p>This sample code demonstrates Spark streaming capabilities by performing a sentiment analysis on a live stream of
 * tweets containing a word sick.</p>
 *
 * <p>In order to run it on your computer, you need to provide a <code>secrets/twitter.properties</code> file with
 * following structure:</p>
 *
 *<pre>
 * consumerKey=YOUR_TWITTER_CONSUMER_KEY
 * consumerSecret=YOUR_TWITTER_CONSUMER_SECRET
 * accessToken=YOUR_TWITTER_ACCESS_TOKEN
 * accessTokenSecret=YOUR_TWITTER_TOKEN_SECRET
 *</pre>
 */
public class SentimentAnalysis {

    /**
     * Path to a file containing Twitter secrets.
     */
    private static final String TWITTER_SECRETS_FILE = "secrets/twitter.properties";

    /**
     * Spark Streaming demo.
     * @param args This code doesn't take any arguments.
     * @throws IOException in case of a missing secrets/twitter.properties file.
     */
    public static void main(String[] args) throws IOException {
        SparkSession spark = SparkSession.builder().appName("TwitterStreaming")
                .master("local[3]")
                .getOrCreate();

        JavaStreamingContext ssc = new JavaStreamingContext(JavaSparkContext.fromSparkContext(spark.sparkContext()), new Duration(5000));

        // We will filter Tweets containing a word sick
        String[] filters = {"sick"};
        JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(ssc, auth(), filters);

        // Uncomment to see list of incomming tweets together with their count per 5s
//        stream.map(status -> new Tweet(status.getCreatedAt(), status.getText()))
//                .foreachRDD(rdd -> {
//                    rdd.foreach(tweet -> {System.out.println(tweet + "");});
//                    Dataset<Row> dataSet = spark.createDataFrame(rdd, Tweet.class);
//                    dataSet.createOrReplaceTempView("tweets");
//                    spark.sql("SELECT COUNT(*) FROM tweets").show();
//                });

        // Load sentiment dataset
        String[] schema = {"word", "sentiment"};
        Dataset<Row> sentiments = spark.read()
                .option("header", "false")
                .option("delimiter", "\t")
                .csv("data/sentiment.tsv")
                .toDF(schema);
        sentiments.registerTempTable("sentiments");

        // For each tweet show its estimated sentiment and the original text
        stream.map(status -> new Tweet(new Date(status.getCreatedAt().getTime()), status.getText()))
                .foreachRDD(rdd -> {
                    Dataset<Row> dataSet = spark.createDataFrame(rdd.rdd(), Tweet.class);
                    dataSet.createOrReplaceTempView("tweets");
                    spark.sql(
                            "SELECT SUM(sentiment), text FROM tweets " +
                            "LEFT JOIN sentiments ON (tweets.text LIKE CONCAT('%', word, '%')) " +
                            "GROUP BY text ORDER BY SUM(sentiment)").show(false);
                });

        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    /**
     * Utility class to represent a simplified tweet structure.
     */
    public static class Tweet {
        private final Date createdAt;
        private final String text;

        public  Tweet(Date createdAt, String text) {
            this.createdAt = createdAt;
            this.text = text;
        }

        public Date getCreatedAt() {
            return createdAt;
        }

        public String getText() {
            return text;
        }

        @Override
        public String toString() {
            return "Tweet{" +
                    "createdAt=" + createdAt +
                    ", text='" + text + '\'' +
                    '}';
        }
    }

    /**
     * Generates OAuth configuration for Twitter API based on Twitter credentials stored in a properties file {@link #TWITTER_SECRETS_FILE}.
     * @return OAuth configuration for Twitter API.
     * @throws IOException in case of a missing file with Twitter secrets.
     */
    private static OAuthAuthorization auth() throws IOException {
        Properties twitterSecrets = loadTwitterSecretsProperties();
        return new OAuthAuthorization(new ConfigurationBuilder()
                .setOAuthConsumerKey(twitterSecrets.getProperty("consumerKey"))
                .setOAuthConsumerSecret(twitterSecrets.getProperty("consumerSecret"))
                .setOAuthAccessToken(twitterSecrets.getProperty("accessToken"))
                .setOAuthAccessTokenSecret(twitterSecrets.getProperty("accessTokenSecret"))
                .build());
    }

    /**
     * Loads Twitter credentials from a properties file.
     * @return Map of Twitter credentials consumerKey, consumerSecret, accessToken, accessTokenSecret
     * @throws IOException In case of a missing {@link #TWITTER_SECRETS_FILE}.
     */
    private static Properties loadTwitterSecretsProperties() throws IOException {
        try(InputStream is = new FileInputStream(TWITTER_SECRETS_FILE)) {
            Properties prop = new Properties();
            prop.load(is);
            return prop;
        }
    }
}
