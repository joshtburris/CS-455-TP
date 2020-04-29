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

public final class TotalNetIncomePerQuarter {

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.out.println("You must pass in at least 1 filepath");
      System.exit(1);
    }

    SparkSession spark = SparkSession
      .builder()
      .appName("TotalNetIncomePerQuarter")
      .getOrCreate();
    
    //Read data from arguments
    JavaRDD<Row> rows = spark.read().csv(args).javaRDD();

    JavaPairRDD<String, Double> totalNetIncomePerQuarter = rows.mapToPair(row -> {
        try {
            String year = row.getString(2);
            String quarter = row.getString(3);
            Double netIncome = Double.parseDouble(row.getString(0));
            return new Tuple2<>(year + "-" + quarter, netIncome);
        }
        catch (NumberFormatException e) {
            return new Tuple2<>("null", 0.0);
        }
        catch (NullPointerException e) {
            return new Tuple2<>("null", 0.0);
        }
    }).reduceByKey((i1, i2) -> i1 + i2).sortByKey();

    //write net income per quarter data to file
    List<Tuple2<String, Double>> netIncomeOutput = totalNetIncomePerQuarter.collect();
    File netIncomeOutputFile = new File("./total-net-income-per-quarter.csv");
    BufferedWriter writer = new BufferedWriter(new FileWriter(netIncomeOutputFile));
    writer.write("DATE" + "," + "NET_INCOME\n");
    for (Tuple2<String, Double> tuple : netIncomeOutput) {
      writer.write(tuple._1() + "," + tuple._2() + "\n");
    }
    writer.close();

    spark.stop();
  } 
}

