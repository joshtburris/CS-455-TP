import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Arrays;
import java.lang.NumberFormatException;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.File;

public final class DomesticPassengersFreightAndMailPerAirline {

  public static void main(String[] args) throws Exception {
    final boolean uniqueCarrierSpecified;
    final String uniqueCarrierArgumentString;
    if (args.length < 1) {
      System.out.println("You must pass in at least 1 filepath");
      System.exit(1);
    }
    if (args[0].equals("-u") && args.length > 1) {
      uniqueCarrierSpecified = true;
      uniqueCarrierArgumentString = args[1];
    }
    else {
      uniqueCarrierSpecified = false;
      uniqueCarrierArgumentString = "";
    }
    if (uniqueCarrierSpecified && args.length < 3) {
      System.out.println("You must pass in at least 1 filepath");
      System.exit(1);
    }

    SparkSession spark = SparkSession
      .builder()
      .appName("DomesticPassengersFreightAndMailPerAirline")
      .getOrCreate();
    
    //Read data from arguments
    String[] filepaths;
    if (uniqueCarrierSpecified) {
      filepaths = Arrays.copyOfRange(args, 2, args.length);
    }
    else {
      filepaths = args;
    }
    JavaRDD<Row> rows = spark.read().csv(filepaths).javaRDD();

    JavaPairRDD<String, Double> airlinePassengersPerMonthDomestic = rows.mapToPair(row -> {
        try {
            if (row.getString(40).equals("DU")){
                String year = row.getString(35);
                String month = row.getString(37);
                if (month.length() == 1) {
                    month = "0" + month;
                }
                String uniqueCarrierName = row.getString(6);
                if (uniqueCarrierSpecified && (!uniqueCarrierName.equals(uniqueCarrierArgumentString))){
                    return new Tuple2<>("null", 0.0);
                }
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
        catch (NullPointerException e) {
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
                if (uniqueCarrierSpecified && (!uniqueCarrierName.equals(uniqueCarrierArgumentString))){
                    return new Tuple2<>("null", 0.0);
                }
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
        catch (NullPointerException e) {
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
            if (uniqueCarrierSpecified && (!uniqueCarrierName.equals(uniqueCarrierArgumentString))){
                return new Tuple2<>("null", 0.0);
            }
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
        catch (NullPointerException e) {
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
}
