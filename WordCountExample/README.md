## Word count example with Tom_Sawyer.txt

### Prerequisites

* You have a local hadoop cluster running
* Add lorem_ipsum.txt to your local hadoop cluster (JavaWordCount example reads from your local hadoop cluster, I believe it looks at the HADOOP_CONF_DIR variable)
* Create a spark cluster with 1 or 2 workers
* SPARK_HOME is set to the path of your spark directory
* Add gradle to your PATH variable: export PATH="/usr/local/gradle/bin:$PATH"


### How to execute

* build the example with 'gradle shadowJar' (uses the shadow plugin)
* execute the command: bash $SPARK_HOME/bin/spark-submit --class org.apache.spark.examples.JavaWordCount --master spark://**master-ip-address**:7077 build/libs/WordCountExample-all.jar /lorem_ipsum.txt
