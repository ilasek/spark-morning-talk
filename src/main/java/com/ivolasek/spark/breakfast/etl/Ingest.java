package com.ivolasek.spark.breakfast.etl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;


/**
 * <p>Run locally with <code>-Dspark.master=local[*]</code>. The first argument is path to the source file.
 * The second argument is a jdbc connection string to a Postgres database.</p>
 * <p>Run on cluster:</p>
 * <pre>
 * spark-submit --master yarn --num-executors 20 --executor-cores 1 --executor-memory 1G\
 *      --class com.ivolasek.spark.breakfast.etl.Ingest \
 *      spark-breakfast-1.0-SNAPSHOT-jar-with-dependencies.jar \
 *      "s3://spark-breakfast/patients_large.csv" "jdbc:postgresql://db.host/spark"
 * </pre>
 *
 * <p>Used dataset can be downloaded from
 * <a href="https://www.kaggle.com/joniarroba/noshowappointments">https://www.kaggle.com/joniarroba/noshowappointments</a></p>
 */
public class Ingest {

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        SparkSession spark = SparkSession.builder().appName("IngestPatientData")
                .getOrCreate();

//        https://www.kaggle.com/joniarroba/noshowappointments
        Dataset<Row> patients = spark.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(args[0]);

        patients.show();
        patients.printSchema();
        System.out.println(patients.count());

        patients.withColumn("PatientID", patients.col("PatientID").cast(DataTypes.LongType))
                .withColumn("Hipertension", patients.col("Hipertension").cast(DataTypes.BooleanType))
                .withColumn("Diabetes", patients.col("Diabetes").cast(DataTypes.BooleanType))
                .withColumn("Handcap", patients.col("Handcap").cast(DataTypes.BooleanType))
                .withColumnRenamed("No-show", "No_show")
                .registerTempTable("patients");

        Dataset<Row> sickPatients = spark.sql("SELECT *, Hipertension OR Diabetes OR Handcap as Sick FROM patients");
        sickPatients.show(10);

        sickPatients.write()
                .format("jdbc")
                .option("url", args[1])
                .option("dbtable", "public.patients")
                .option("user", "spark")
                .option("password", "spark")
                .option("driver", "org.postgresql.Driver")
                .mode(SaveMode.Overwrite)
                .save();

//        Dataset<Row> neighbourHoods = spark.sql("SELECT COUNT(*), Neighbourhood FROM patients GROUP BY Neighbourhood");
//        neighbourHoods.show();

        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        System.out.println("Elaspsed time: " + elapsedTime);
    }
}
