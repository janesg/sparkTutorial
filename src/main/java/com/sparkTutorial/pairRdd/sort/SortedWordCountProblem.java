package com.sparkTutorial.pairRdd.sort;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class SortedWordCountProblem {

    /* Create a Spark program to read the an article from in/word_count.text,
       output the number of occurrence of each word in descending order.

       Sample output:

       apple : 200
       shoes : 193
       bag : 176
       ...
     */

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/word_count.text");

        // There is currently no support in Scala for sorting by value
        // I want the output sorted by:
        //  - descending value
        //  and then where values are equal...
        //  - ascending key
        // We first sort by ascending key, then swap key & value, sort
        // by descending key (which is really the value) and then swap again
        JavaPairRDD<String, Integer> wordCounts = lines
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((x, y) -> x + y)
                .sortByKey()
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .mapToPair(Tuple2::swap);

        for (Tuple2<String, Integer> wordCountPair : wordCounts.collect()) {
            System.out.println(wordCountPair._1() + " : " + wordCountPair._2());
        }
    }
}

