package com.sparkTutorial.pairRdd.aggregation.reducebykey.housePrice;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class AverageHousePriceProblem {

    public static void main(String[] args) {

        /* Create a Spark program to read the house data from in/RealEstate.csv,
           output the average price for houses with different number of bedrooms.

        The houses dataset contains a collection of recent real estate listings in San Luis Obispo county and
        around it. 

        The dataset contains the following fields:
        1. MLS: Multiple listing service number for the house (unique ID).
        2. Location: city/town where the house is located. Most locations are in San Luis Obispo county and
        northern Santa Barbara county (Santa Maria­Orcutt, Lompoc, Guadelupe, Los Alamos), but there
        some out of area locations as well.
        3. Price: the most recent listing price of the house (in dollars).
        4. Bedrooms: number of bedrooms.
        5. Bathrooms: number of bathrooms.
        6. Size: size of the house in square feet.
        7. Price/SQ.ft: price of the house per square foot.
        8. Status: type of sale. Thee types are represented in the dataset: Short Sale, Foreclosure and Regular.

        Each field is comma separated.

        Sample output:

           (3, 325000)
           (1, 266356)
           (2, 325000)
           ...

           3, 1 and 2 mean the number of bedrooms. 325000 means the average price of houses with 3 bedrooms is 325000.
         */

        Logger.getLogger("org").setLevel(Level.ERROR);
        // Set master options:
        //  - local     : 1 core
        //  - local[2]  : 2 cores
        //  - local[*]  : all cores available
        SparkConf conf = new SparkConf().setAppName("airportsNotInUSA").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> houses = sc.textFile("in/RealEstate.csv");
        JavaPairRDD<Integer, AvgCount> bedAvgCountPrice =
                houses
                        .filter(house -> !house.startsWith("MLS"))
                        .mapToPair(house -> {
                            String[] fields = house.split(",");
                            return new Tuple2<>(Integer.valueOf(fields[3]),
                                                new AvgCount(1, Double.valueOf(fields[2])));
                        })
                        .reduceByKey((avgCountX, avgCountY) ->
                                new AvgCount(avgCountX.getCount() + avgCountY.getCount(),
                                             avgCountX.getTotal() + avgCountY.getTotal()));

        JavaPairRDD<Integer, Double> bedHousePriceAvg =
                bedAvgCountPrice.mapValues(avgCount -> avgCount.getTotal() / avgCount.getCount());

        // Extra challenge : display in reverse order of average price (highest to lowest)
        for (Map.Entry<Integer, Double> entry :
                bedHousePriceAvg.collectAsMap()
                        .entrySet()
                        .stream()
                        .sorted((Map.Entry<Integer, Double> o1, Map.Entry<Integer, Double> o2) ->
                                o1.getValue().equals(o2.getValue()) ?
                                        o1.getKey().compareTo(o2.getKey()) : o2.getValue().compareTo(o1.getValue()))
                        .collect(Collectors
                                .toMap(Map.Entry::getKey, Map.Entry::getValue,
                                        (e1, e2) -> e2,
                                        LinkedHashMap::new))
                        .entrySet()
                ) {
            System.out.println(String.format("Beds : %d \t - Av. Price : %,.2f", entry.getKey(), entry.getValue()));
        }

    }

}
