import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.lang.NumberFormatException;

public final class PassengerAnalysis {
  private static final Pattern COMMA = Pattern.compile(",");

  public static void main(String[] args) throws Exception {

    SparkSession spark = SparkSession
      .builder()
      .appName("PassengerAnalysis")
      .getOrCreate();

    //JavaRDD<String> lines = spark.read().textFile("/TP/1990.csv").javaRDD();
    
    JavaRDD<String> lines = spark.read().textFile("/TP/1990.csv", "/TP/1991.csv", "/TP/1992.csv", "/TP/1993.csv", "/TP/1994.csv", "/TP/1995.csv",
        "/TP/1996.csv", "/TP/1997.csv", "/TP/1998.csv", "/TP/1999.csv", "/TP/2000.csv", "/TP/2001.csv", "/TP/2002.csv", "/TP/2003.csv",
        "/TP/2004.csv", "/TP/2005.csv", "/TP/2006.csv", "/TP/2007.csv", "/TP/2008.csv", "/TP/2009.csv", "/TP/2010.csv", "/TP/2011.csv",
        "/TP/2012.csv", "/TP/2013.csv", "/TP/2014.csv", "/TP/2015.csv", "/TP/2016.csv", "/TP/2017.csv", "/TP/2018.csv", "/TP/2019.csv").javaRDD();

    JavaRDD<List<String>> entries = lines.map(s -> Arrays.asList(COMMA.split(s)));   

    JavaPairRDD<String, Double> passengersPerMonth = entries.mapToPair(s -> {
        String year = PassengerAnalysis.findYear(s);
        if (year != "") {
            try {
                int indexOfYear = s.indexOf(year);
                int sizeOfList = s.size();
                String month = s.get(sizeOfList - 3);
                if (month.length() == 1) {
                    month = "0" + month;
                }
                return new Tuple2<>(s.get(indexOfYear) + "-" + month, Double.parseDouble(s.get(0)));
            }
            catch (NumberFormatException e) {
                return new Tuple2<>("null", 0.0);
            }
        }
        else {
            return new Tuple2<>("null", 0.0);
        }
    });

    JavaPairRDD<String, Double> sortedCounts = passengersPerMonth.reduceByKey((i1, i2) -> i1 + i2).sortByKey();

    List<Tuple2<String, Double>> output = sortedCounts.collect();
    for (Tuple2<?,?> tuple : output) {
      System.out.println(tuple._1() + "," + tuple._2());
    }
    spark.stop();
  }

  private static String findYear(List<String> s) {
      for (int i=1990; i <= 2019; i++) {
        String stringRepresentation = Integer.toString(i);
        if (s.contains(stringRepresentation)) {
          return stringRepresentation;
        }
      }
      return "";
  } 
}
