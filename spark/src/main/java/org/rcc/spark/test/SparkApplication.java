package org.rcc.spark.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;



public class SparkApplication {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("spark rdd test")
                .set("spark.eventLog.enabled", "true")
                .set("spark.eventLog.dir", "spark-logs")
                .set("spark.hadoop.validateOutputSpecs", "false")
                .setMaster("local[8]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.textFile("input.txt")
                .flatMap(s -> Arrays.asList(s.split("[^\\p{L}]+")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b)
                .map(tuple -> tuple._1() + " " + tuple._2())
                .coalesce(1)
                .saveAsTextFile("wordcounts");

    }
}
