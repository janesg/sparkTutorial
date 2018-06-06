package com.sparkTutorial.rdd.nasaApacheWebLogs;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.text.SimpleDateFormat;
import java.util.Date;

public class SameHostsProblem {

    public static void main(String[] args) throws Exception {

        /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
           "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
           Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
           Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

           Example output:
           vagrant.vf.mmc.com
           www-a1.proxy.aol.com
           .....

           Keep in mind, that the original log files contains the following header lines.
           host	logname	time	method	url	response	bytes

           Make sure the head lines are removed in the resulting RDD.
         */
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("unionLog").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> julHosts =
                sc.textFile("in/nasa_19950701.tsv")
                        .filter(line -> !line.startsWith("host\tlogname"))
                        .map(line -> line.split("\t")[0]);

        JavaRDD<String> augHosts =
                sc.textFile("in/nasa_19950801.tsv")
                        .filter(line -> !line.startsWith("host\tlogname"))
                        .map(line -> line.split("\t")[0]);

        JavaRDD<String> sameHosts = julHosts.intersection(augHosts);

        String timestamp = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss").format(new Date());
        sameHosts.saveAsTextFile(String.format("out/nasa_logs_same_hosts-%s.csv", timestamp));
    }
}
