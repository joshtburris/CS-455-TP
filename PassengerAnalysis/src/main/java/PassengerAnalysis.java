import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.lang.NumberFormatException;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.File;

public final class PassengerAnalysis {

  public static void main(String[] args) throws Exception {

    SparkSession spark = SparkSession
      .builder()
      .appName("PassengerAnalysis")
      .getOrCreate();
    
    //test case
    //JavaRDD<Row> rows = spark.read().csv("/TP/1990.csv").javaRDD();
    
    JavaRDD<Row> rows = spark.read().csv("/TP/1990.csv", "/TP/1991.csv", "/TP/1992.csv", "/TP/1993.csv", "/TP/1994.csv", "/TP/1995.csv",
        "/TP/1996.csv", "/TP/1997.csv", "/TP/1998.csv", "/TP/1999.csv", "/TP/2000.csv", "/TP/2001.csv", "/TP/2002.csv", "/TP/2003.csv",
        "/TP/2004.csv", "/TP/2005.csv", "/TP/2006.csv", "/TP/2007.csv", "/TP/2008.csv", "/TP/2009.csv", "/TP/2010.csv", "/TP/2011.csv",
        "/TP/2012.csv", "/TP/2013.csv", "/TP/2014.csv", "/TP/2015.csv", "/TP/2016.csv", "/TP/2017.csv", "/TP/2018.csv", "/TP/2019.csv").javaRDD();

    JavaPairRDD<String, Double> airlinePassengersPerMonth = rows.mapToPair(row -> {
        try {
            String month = row.getString(33);
            if (month.length() == 1) {
                month = "0" + month;
            }
            return new Tuple2<>(row.getString(31) + "-" + month + ",\"" + row.getString(6) + "\"", Double.parseDouble(row.getString(0)));
        }
        catch (NumberFormatException e) {
            return new Tuple2<>("null", 0.0);
        }
    }).reduceByKey((i1, i2) -> i1 + i2).sortByKey();

    List<Tuple2<String, Double>> output = airlinePassengersPerMonth.collect();
    File outputFile = new File("./airline-passenger-output.csv");
    BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
    for (Tuple2<?,?> tuple : output) {
      writer.write(tuple._1() + "," + tuple._2() + "\n");
    }
    writer.close();
    spark.stop();
  } 
}
