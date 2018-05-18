package com.sparkTutorial.rdd;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.*;
import java.util.stream.Collectors;

public class WordCount {

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/word_count.text");
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

//        Map<String, Long> wordCounts = words.countByValue();

//        for (Map.Entry<String, Long> entry : wordCounts.entrySet()) {
//            System.out.println(entry.getKey() + " : " + entry.getValue());
//        }

        // Sort output map by descending count value only
        // Collected into LinkedHashMap to maintain order
//        Map<String, Long> wordCounts = words.countByValue()
//                .entrySet()
//                .stream()
//                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
//                .collect(
//                        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
//                                         (e1, e2) -> e2,
//                                         LinkedHashMap::new));

        // Use custom comparator to sort word count output map by:
        //      - 1st: descending count value
        //      - 2nd: ascending word key
        //
        // Useful link:
        //  - https://www.javacodegeeks.com/2017/09/java-8-sorting-hashmap-values-ascending-descending-order.html
        //
        // Note: has to be collected into LinkedHashMap to maintain order
        //
        Map<String, Long> wordCounts = words.countByValue()
                .entrySet()
                .stream()
                .sorted((Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) -> {
                    return o1.getValue().equals(o2.getValue()) ?
                            o1.getKey().compareTo(o2.getKey())
                            : o2.getValue().compareTo(o1.getValue());
                })
                .collect(
                        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                                (e1, e2) -> e2,
                                LinkedHashMap::new));

        for (Map.Entry<String, Long> entry : wordCounts.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }

    }
}
