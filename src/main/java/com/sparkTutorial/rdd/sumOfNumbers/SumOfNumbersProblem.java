package com.sparkTutorial.rdd.sumOfNumbers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

public class SumOfNumbersProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
           print the sum of those numbers to console.

           Each row of the input file contains 10 prime numbers separated by spaces.
         */

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("sumNumbers").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> primeNums =
                sc.textFile("in/prime_nums.text")
                        .flatMap(line -> Arrays.asList(line.split("\t")).iterator())
                        .map(numStr -> Integer.valueOf(numStr.trim()));

        Integer sum = primeNums.reduce((x, y) -> x + y);

        System.out.println("sum is : " + sum);
    }
}
