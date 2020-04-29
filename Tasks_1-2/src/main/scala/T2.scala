import scala.util.{Try, Success, Failure}
import scala.io.Source
import scala.util.control.NonFatal
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object Task2 {

	def main(args: Array[String]) {
		val spark = SparkSession.builder.appName("Task2").getOrCreate()
		val sc = SparkContext.getOrCreate()
		import spark.implicits._

		var distMap:Map[String,Long] = Map()

                for (fileYear <- 1990 until 2020) {
			var filename = "hdfs://boise:30221/tp/BTS/t100_market_" + fileYear + ".csv"

			sc.textFile(filename).collect().foreach{ line =>
				try {
					val cols = parseCSV(line)
					var dist:Long = cols(3).toDouble.asInstanceOf[Number].longValue
					// key = region + month + /01/ + year
					val key = cols(40) +","+ cols(37) +"/01/"+ cols(35)

					if (distMap.contains(key)) {
						val get:Long = distMap.getOrElse(key, 0)
						dist += get
					}
					distMap += (key -> dist)

				} catch { case NonFatal(t) => }
			}
		}

		sc.parallelize(distMap.toSeq).saveAsTextFile("hdfs://boise:30221/tp/T2-out")
	}

	def parseCSV(line:String):Array[String] = {
		var splits:Array[String] = Array()
		var buf = ""
		var inQuotes:Boolean = false

		for (c <- line) {
			if (inQuotes) {
				if (c == '\"')
					inQuotes = false
				else
					buf = buf.concat(c.toString)
			} else {
				if (c == ',') {
					splits = splits :+ buf
					buf = ""
				} else if (c == '\"') {
					inQuotes = true
				} else
					buf += c
			}
		}
		splits :+ buf
		return splits
	}
}
