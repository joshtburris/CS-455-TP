import org.apache.spark.sql.SparkSession

object PercentCancellation {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("PercentCancellation").getOrCreate()
    import spark.implicits._

    val textFile = spark.read.format("csv").option("header",true).load("hdfs://juneau:11111/flights/*.csv")
    
    val data = textFile.rdd.map{ row =>
      (row.getString(1),(1.toDouble,row.getString(9).toDouble))
    }.reduceByKey{ (x,y) =>
      (x._1+y._1, x._2+y._2)
    }.map{ row =>
      (row._1, row._2._2 / row._2._1)
    }.sortBy(_._1)

    data.toDF.write.format("csv").save("hdfs://juneau:11111/results/percentCancellation")
  }
}
