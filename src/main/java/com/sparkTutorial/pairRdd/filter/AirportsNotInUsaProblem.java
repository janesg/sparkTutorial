package com.sparkTutorial.pairRdd.filter;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.Date;

public class AirportsNotInUsaProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text;
           generate a pair RDD with airport name being the key and country name being the value.
           Then remove all the airports which are located in United States and output the pair RDD to out/airports_not_in_usa_pair_rdd.text

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located,
           IATA/FAA code, ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:

           ("Kamloops", "Canada")
           ("Wewak Intl", "Papua New Guinea")
           ...
         */

        Logger.getLogger("org").setLevel(Level.ERROR);
        // Set master options:
        //  - local     : 1 core
        //  - local[2]  : 2 cores
        //  - local[*]  : all cores available
        SparkConf conf = new SparkConf().setAppName("airportsNotInUSA").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airports = sc.textFile("in/airports.text");
        JavaPairRDD<String, String> nonUsaAirports =
                // Utils.COMMA_DELIMITER contains regex that ignores comma between double quotes
                // Use replaceAll to strip off the double quotes from start and end of country string

                airports
                        .mapToPair(airport -> {
                            String[] fields = airport.split(Utils.COMMA_DELIMITER);
                            return new Tuple2<>(fields[1], fields[3]);
                        })
                        .filter(tuple -> !tuple._2.equals("\"United States\""));

                // Doesn't follow spec exactly but is it more effective to filter
                // the RDD first and create PairRDD from the result
//                airports
//                        .filter(airport ->
//                                !airport.split(Utils.COMMA_DELIMITER)[3]
//                                        .replaceAll("^\"|\"$", "")
//                                        .equals("United States"))
//                        .mapToPair(airport -> {
//                            String[] fields = airport.split(Utils.COMMA_DELIMITER);
//                            return new Tuple2<>(fields[1], fields[3]);
//                        });

        // As we specified running on 1 core, 'output file' is actually a directory
        // containing 1 actual output file
        String timestamp = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss").format(new Date());
        nonUsaAirports.saveAsTextFile(String.format("out/airports_not_in_usa_pair_rdd-%s.text", timestamp));

    }
}
