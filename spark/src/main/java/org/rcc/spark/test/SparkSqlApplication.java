package org.rcc.spark.test;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.count;


public class SparkSqlApplication {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("spark sql test")
                .set("spark.eventLog.enabled", "true")
                .set("spark.eventLog.dir", "spark-logs")
                .set("spark.hadoop.validateOutputSpecs", "false")
                .setMaster("local[8]");


        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        spark.read()
                .textFile("input.txt")
                .flatMap(s -> Arrays.asList(s.split("[^\\p{L}]+")).iterator(), Encoders.STRING())
                .groupBy("value").agg(count("value").as("count"))
                .select(concat(col("value"), col("count")))
                .coalesce(1)
                .write()
                    .mode(SaveMode.Overwrite)
                    .text("wordcounts");

    }
}
