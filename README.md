#Spark Introduction
This demo has been originally prepared for Datascript Spark Morning Talk (http://www.datascript.cz/morning-talks/apache-spark-jak-to-jiskri-v-msd/).

## Content of this repo

* [SparkIntro.pptx](SparkIntro.pptx) - Spark Introduction presentation
* [Sample ETL job](src/main/java/com/ivolasek/spark/breakfast/etl/Ingest.java)
* [Sample Spark Streaming job](src/main/java/com/ivolasek/spark/breakfast/streaming/SentimentAnalysis.java) for sentiment analysis of tweets
* [Data](data) used in examples
* [database](database) SQL scripts used to generate target postgresSQL database
* [run_zeppelin.sh](run_zeppelin.sh) script starting Zeppelin notebook using Docker

## How to run ETL
The code assumes you have a Postgres running with a database called spark.

```
mvn celan package
scp -i your_ssh_key.pem target/spark-breakfast-1.0-SNAPSHOT-jar-with-dependencies.jar user@your.target.culster.com:
```

On the cluster:
```
spark-submit --master yarn --num-executors 20 --executor-cores 1 --executor-memory 1G\
    --class com.ivolasek.spark.breakfast.etl.Ingest \
    spark-breakfast-1.0-SNAPSHOT-jar-with-dependencies.jar \
    "s3://bucket/path/to/your.csv" "jdbc:postgresql://dbhost/spark"
```

When developing in your IDE, switch on the IDE maven profile in order to resolve Spark dependencies.

## How to start Zeppelin Notebook
You have to have Docker installed.
```
./run_zeppelin.sh
```

Then access [http://localhost:8080](http://localhost:8080).

## Contacts
In case of any questions feel free to reach out to me:
* [@ilasek](http://www.twitter.com/ilasek)
* [ivolasek.com](http://www.ivolasek.com/)
* [LinkedIn](http://cz.linkedin.com/in/ivolasek)

