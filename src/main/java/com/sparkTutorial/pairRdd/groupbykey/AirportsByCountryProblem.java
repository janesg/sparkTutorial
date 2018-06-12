package com.sparkTutorial.pairRdd.groupbykey;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class AirportsByCountryProblem {

    public static void main(String[] args) {

        /* Create a Spark program to read the airport data from in/airports.text,
           output the the list of the names of the airports located in each country.

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:

           "Canada", ["Bagotville", "Montreal", "Coronation", ...]
           "Norway" : ["Vigra", "Andenes", "Alta", "Bomoen", "Bronnoy",..]
           "Papua New Guinea",  ["Goroka", "Madang", ...]
           ...
         */

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("airportsUpperCase").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airports = sc.textFile("in/airports.text");
        JavaPairRDD<String, Iterable<String>> airportNamesByCountry =
                // Utils.COMMA_DELIMITER contains regex that ignores comma between double quotes
                airports
                        .mapToPair(airport -> {
                            String[] fields = airport.split(Utils.COMMA_DELIMITER);
                            return new Tuple2<>(fields[3], fields[1]);
                        })
                        .groupByKey();

        // Display output sorted by key using natural order
        // and with each list of airport names also sorted in natural order and with duplicates removed
        for (Map.Entry<String, List<String>> entry :
                airportNamesByCountry.collectAsMap().entrySet()
                    .stream()
                    .sorted(Comparator.comparing(Map.Entry::getKey))
                    .collect(Collectors.toMap(
                                entry -> entry.getKey(),
                                entry -> StreamSupport.stream(entry.getValue().spliterator(), false)
                                    .distinct()
                                    .sorted()
                                    .collect(Collectors.toList()),
                                (e1, e2) -> e2,
                                LinkedHashMap::new))
                    .entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue().toString());
        }
    }
}
