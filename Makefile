build:
	gradle shadowJar

wordcount:
	$(MAKE) -B build
	${SPARK_HOME}/bin/spark-submit --class org.apache.spark.examples.JavaWordCount --master spark://boise:7077 build/libs/WordCountExample-all.jar /lorem_ipsum.txt
