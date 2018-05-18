package com.sparkTutorial.rdd.airports;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;

public class AirportsByLatitudeProblem {

    private static final BigDecimal LATITUDE = new BigDecimal("40");

    public static void main(String[] args) {

        /* Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
           Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:
           "St Anthony", 51.391944
           "Tofino", 49.082222
           ...
         */

        Logger.getLogger("org").setLevel(Level.ERROR);
        // Set master options:
        //  - local     : 1 core
        //  - local[2]  : 2 cores
        //  - local[*]  : all cores available
        SparkConf conf = new SparkConf().setAppName("airportsByAltitude").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airports = sc.textFile("in/airports.text");

        JavaRDD<String> usaAirports =
                // Utils.COMMA_DELIMITER contains regex that ignores comma between double quotes
                // Use replaceAll to strip off the double quotes from start and end of country string
                airports
                        .filter(airport ->
                            new BigDecimal(airport.split(Utils.COMMA_DELIMITER)[6]).compareTo(LATITUDE) > 0)
                        .map(airport -> {
                            String[] fields = airport.split(Utils.COMMA_DELIMITER);
                            return String.join(" , ", fields[1], fields[6]);
                        });

        // As we specified running on 2 cores, 'output file' is actually a directory
        // containing 2 actual output files
        String timestamp = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss").format(new Date());
        usaAirports.saveAsTextFile(String.format("out/airports_by_altitude-%s.text", timestamp));
    }
}
