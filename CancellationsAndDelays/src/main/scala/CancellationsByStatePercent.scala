import org.apache.spark.sql.SparkSession

object CancellationsByStatePercent {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("CancellationsByStatePercent").getOrCreate()
    import spark.implicits._

    val textFile = spark.read.format("csv").option("header",true).load("hdfs://juneau:11111/flights/*.csv")
    
    val data = textFile.rdd.map{ row =>
      // (state, (cancellations, count)
      (row.getString(4), (row.getString(9).toDouble, 1.toDouble))
    }.reduceByKey{ (x,y) =>
      (x._1+y._1,x._2+y._2)
    }.map{ row =>
      (row._1, row._2._1/row._2._2)
    }.sortBy(_._1)

    data.toDF.write.format("csv").save("hdfs://juneau:11111/results/cancellationsByStatePercent")
  }
}
