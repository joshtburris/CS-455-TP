import org.apache.spark.sql.SparkSession

object DailyDelays {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("DailyDelays").getOrCreate()
    import spark.implicits._

    val textFile = spark.read.format("csv").option("header",true).load("hdfs://juneau:11111/flights/*.csv")
    
    val data = textFile.rdd.map{ row =>
      // (Date, (departureDelay, arrivalDelay, count))
      (row.getString(1), (row.getString(7).toDouble,row.getString(8).toDouble,1.toDouble))
    }.reduceByKey{ (x,y) =>
      (x._1+y._1, x._2+y._2, x._3+y._3)
    }.map{ row =>
      (row._1, row._2._1/row._2._3, row._2._2/row._2._3)
    }.sortBy(_._1)

    data.toDF.write.format("csv").save("hdfs://juneau:11111/results/dailyDelays")
  }
}
