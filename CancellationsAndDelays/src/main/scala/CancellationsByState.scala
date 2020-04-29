import org.apache.spark.sql.SparkSession

object CancellationsByState {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("CancellationsByState").getOrCreate()
    import spark.implicits._

    val textFile = spark.read.format("csv").option("header",true).load("hdfs://juneau:11111/flights/*.csv")
    
    val data = textFile.rdd.map{ row =>
      // (state, cancellations)
      (row.getString(4), row.getString(9).toDouble.toLong)
    }.reduceByKey{ (x,y) => 
      x+y
    }.sortBy(_._1)

    data.toDF.write.format("csv").save("hdfs://juneau:11111/results/cancellationsByState")
  }
}
