import scala.util.{Try, Success, Failure}
import scala.io.Source
import scala.util.control.NonFatal
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object Task3 {

	def main(args: Array[String]) {
		val spark = SparkSession.builder.appName("Task3").getOrCreate()
		val sc = SparkContext.getOrCreate()
		import spark.implicits._

		var passengerMap:Map[String,Long] = Map()

		val filename = "hdfs://boise:30221/tp/BTS"

		sc.textFile(filename).collect().foreach{ line =>
			try {
				val cols = parseCSV(line)
				var passengerValue:Long = cols(0).toDouble.asInstanceOf[Number].longValue
				// key = region + airline name + year + month
				val key = cols(8) +","+ cols(6) +","+ cols(35) +"-"+ cols(37)

				if (passengerMap.contains(key)) {
					val get:Long = passengerMap.getOrElse(key, 0)
					passengerValue += get
				}
				passengerMap += (key -> passengerValue)

			} catch { case NonFatal(t) => }
		}

		sc.parallelize(passengerMap.toSeq).saveAsTextFile("hdfs://boise:30221/tp/T3-out")
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
