package com.sparkTutorial.pairRdd.mapValues;

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

public class AirportsUppercaseProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text, generate a pair RDD with airport name
           being the key and country name being the value. Then convert the country name to uppercase and
           output the pair RDD to out/airports_uppercase.text

           Each row of the input file contains the following columns:

           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:

           ("Kamloops", "CANADA")
           ("Wewak Intl", "PAPUA NEW GUINEA")
           ...
         */

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("airportsUpperCase").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airports = sc.textFile("in/airports.text");
        JavaPairRDD<String, String> upperAirports =
                // Utils.COMMA_DELIMITER contains regex that ignores comma between double quotes
                airports
                        .mapToPair(airport -> {
                            String[] fields = airport.split(Utils.COMMA_DELIMITER);
                            return new Tuple2<>(fields[1], fields[3]);
                        })
                        .mapValues(String::toUpperCase);

        // As we specified running on 1 core, 'output file' is actually a directory
        // containing 1 actual output file
        String timestamp = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss").format(new Date());
        upperAirports.saveAsTextFile(String.format("out/airports_uppercase-%s.text", timestamp));

    }
}
