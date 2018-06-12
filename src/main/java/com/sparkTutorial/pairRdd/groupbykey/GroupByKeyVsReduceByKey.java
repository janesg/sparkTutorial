package com.sparkTutorial.pairRdd.groupbykey;

import com.google.common.collect.Iterables;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class GroupByKeyVsReduceByKey {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("GroupByKeyVsReduceByKey").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> words = Arrays.asList("one", "two", "two", "three", "three", "three");
        JavaPairRDD<String, Integer> wordsPairRdd = sc.parallelize(words).mapToPair(word -> new Tuple2<>(word, 1));

        // Relationship between RDD, partition and node:
        //  - a single RDD has one or more partitions scattered across multiple nodes,
        //  - a single partition is processed on a single node,
        //  - a single node can handle multiple partitions (with optimum 2-4 partitions per CPU according
        //    to the official documentation)
        //
        // Since Spark supports pluggable resource management details of the distribution will depend on the
        // one you use (Standalone, Yarn, Messos).
        //
        // ReduceByKey is much more efficient with larger data sets:
        //  - Initially, entries are randomly distributed across partitions
        //  - Within each partition, entries with a common key are reduced to a single entry
        //    thus decreasing the overall number of entries
        //  - This reduced number of entries is then shuffled across partitions such that entries
        //    with a common key all exist on a single partition
        //  - ReduceByKey is called a 2nd time on each partition to produce final result for each key
        //
        // Shuffling requires data to be moved between partitions over the network.
        // It is the reduction in the number of entries (compared with GroupByKey) that makes
        // ReduceByKey more efficient.

        List<Tuple2<String, Integer>> wordCountsWithReduceByKey = wordsPairRdd.reduceByKey((x, y) -> x + y).collect();
        System.out.println("wordCountsWithReduceByKey: " + wordCountsWithReduceByKey);

        List<Tuple2<String, Integer>> wordCountsWithGroupByKey = wordsPairRdd.groupByKey()
                .mapValues(intIterable -> Iterables.size(intIterable)).collect();
        System.out.println("wordCountsWithGroupByKey: " + wordCountsWithGroupByKey);
    }
}

