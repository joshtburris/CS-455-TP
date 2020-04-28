import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;

import java.util.List;
import java.lang.NumberFormatException;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.File;

public final class DomesticPassengersFreightAndMailPerAirline {

  public static void main(String[] args) throws Exception {

    SparkSession spark = SparkSession
      .builder()
      .appName("DomesticPassengersFreightAndMailPerAirline")
      .getOrCreate();
    
    //test case
    //JavaRDD<Row> rows = spark.read().csv(args[0]).javaRDD();
    
    JavaRDD<Row> rows = spark.read().csv("/TP/t100_market/t100_market_1990.csv","/TP/t100_market/t100_market_1991.csv","/TP/t100_market/t100_market_1992.csv","/TP/t100_market/t100_market_1993.csv","/TP/t100_market/t100_market_1994.csv","/TP/t100_market/t100_market_1995.csv","/TP/t100_market/t100_market_1996.csv","/TP/t100_market/t100_market_1997.csv","/TP/t100_market/t100_market_1998.csv","/TP/t100_market/t100_market_1999.csv","/TP/t100_market/t100_market_2000.csv","/TP/t100_market/t100_market_2001.csv","/TP/t100_market/t100_market_2002.csv","/TP/t100_market/t100_market_2003.csv","/TP/t100_market/t100_market_2004.csv","/TP/t100_market/t100_market_2005.csv","/TP/t100_market/t100_market_2006.csv","/TP/t100_market/t100_market_2007.csv","/TP/t100_market/t100_market_2008.csv","/TP/t100_market/t100_market_2009.csv","/TP/t100_market/t100_market_2010.csv","/TP/t100_market/t100_market_2011.csv","/TP/t100_market/t100_market_2012.csv","/TP/t100_market/t100_market_2013.csv","/TP/t100_market/t100_market_2014.csv","/TP/t100_market/t100_market_2015.csv","/TP/t100_market/t100_market_2016.csv","/TP/t100_market/t100_market_2017.csv","/TP/t100_market/t100_market_2018.csv","/TP/t100_market/t100_market_2019.csv").javaRDD();

    JavaPairRDD<String, Double> airlinePassengersPerMonthDomestic = rows.mapToPair(row -> {
        try {
            if (row.getString(40).equals("DU")){
                String year = row.getString(35);
                String month = row.getString(37);
                if (month.length() == 1) {
                    month = "0" + month;
                }
                String uniqueCarrierName = row.getString(6);
                Double passengers = Double.parseDouble(row.getString(0));
                return new Tuple2<>(year + "-" + month + ",\"" + uniqueCarrierName + "\"", passengers);
            }
            else {
                return new Tuple2<>("null", 0.0);
            }
        }
        catch (NumberFormatException e) {
            return new Tuple2<>("null", 0.0);
        }
    }).reduceByKey((i1, i2) -> i1 + i2);

    JavaPairRDD<String, Double> airlineFreightPerMonthDomestic = rows.mapToPair(row -> {
        try {
            if (row.getString(40).equals("DU")) {
                String year = row.getString(35);
                String month = row.getString(37);
                if (month.length() == 1) {
                    month = "0" + month;
                }
                String uniqueCarrierName = row.getString(6);
                Double freight = Double.parseDouble(row.getString(1));
                return new Tuple2<>(year + "-" + month + ",\"" + uniqueCarrierName + "\"", freight);
            }
            else {
                return new Tuple2<>("null", 0.0);
            }
        }
        catch (NumberFormatException e) {
            return new Tuple2<>("null", 0.0);
        }
    }).reduceByKey((i1, i2) -> i1 + i2);

    JavaPairRDD<String, Double> airlineMailPerMonthDomestic = rows.mapToPair(row -> {
        try {
            if (row.getString(40).equals("DU")) {
            String year = row.getString(35);
            String month = row.getString(37);
            if (month.length() == 1) {
                month = "0" + month;
            }
            String uniqueCarrierName = row.getString(6);
            Double mail = Double.parseDouble(row.getString(2));
            return new Tuple2<>(year + "-" + month + ",\"" + uniqueCarrierName + "\"", mail);
            }
            else {
                return new Tuple2<>("null", 0.0);
            }
        }
        catch (NumberFormatException e) {
            return new Tuple2<>("null", 0.0);
        }
    }).reduceByKey((i1, i2) -> i1 + i2);

    //write domestic data to file
    JavaPairRDD<String, Tuple2<Tuple2<Double, Double>, Double>> airlinePassengersFreightAndMailPerMonthDomestic = airlinePassengersPerMonthDomestic.join(airlineFreightPerMonthDomestic).join(airlineMailPerMonthDomestic).sortByKey();

    List<Tuple2<String, Tuple2<Tuple2<Double, Double>, Double>>> domesticOutput = airlinePassengersFreightAndMailPerMonthDomestic.collect();
    File domesticOutputFile = new File("./domestic-airline-passenger-freight-mail.csv");
    BufferedWriter writerDomestic = new BufferedWriter(new FileWriter(domesticOutputFile));
    writerDomestic.write("DATE" + "," + "UNIQUE_CARRIER_NAME" + "," + "TOTAL_PASSENGERS" + "," + "TOTAL_FREIGHT" + "," + "TOTAL_MAIL\n");
    for (Tuple2<String,Tuple2<Tuple2<Double, Double>, Double>> tuple : domesticOutput) {
      writerDomestic.write(tuple._1() + "," + tuple._2()._1()._1() + "," + tuple._2()._1()._2() + "," + tuple._2()._2() + "\n");
    }
    writerDomestic.close();

    spark.stop(); 
}
