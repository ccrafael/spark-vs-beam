package org.rcc.beam.test;

import org.apache.beam.runners.spark.SparkContextOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class BeamApplication {


    public static void main(String[] args) {

        // Start by defining the options for the pipeline.
        PipelineOptions o = PipelineOptionsFactory.fromArgs(args)
                .withValidation().create();

        SparkContextOptions options = o.as(SparkContextOptions.class);

        SparkConf conf = new SparkConf().setAppName("beam test")
                .set("spark.eventLog.enabled", "true")
                .set("spark.eventLog.dir", "spark-logs")
                .set("spark.hadoop.validateOutputSpecs", "false")
                .setMaster("local[8]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        options.setProvidedSparkContext(sc);
        options.setUsesProvidedSparkContext(true);
        options.setRunner(SparkRunner.class);

        // Then create the pipeline.
        Pipeline p = Pipeline.create(options);

        p.apply("ReadLines",
                TextIO.read().from("input.txt"))
                .apply("GetWords",
                        FlatMapElements.into(TypeDescriptors.strings()).via(s -> Arrays.asList(s.split("[^\\p{L}]+"))))
                .apply(Count.<String>perElement())
                .apply("FormatResults",
                        MapElements.into(TypeDescriptors.strings())
                                .via((KV<String, Long> wordCount) -> wordCount.getKey() + ": " + wordCount.getValue()))

                .apply(TextIO.write().to("wordcounts"));


        PipelineResult result = p.run();

        result.waitUntilFinish();
    }
}
