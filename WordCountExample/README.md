## Word count example with Tom_Sawyer.txt

### Prerequisites

* Make sure you have a local hadoop cluster running
* Add Tom_Sawyer.txt to your local hadoop cluster (JavaWordCount example reads from your local hadoop cluster)
* Create a spark cluster with 1 or 2 workers


### How to execute

* build the example with 'gradle shadowJar' (uses the shadow plugin)
* execute the command: bash $SPARK_HOME/bin/spark-submit --class org.apache.spark.examples.JavaWordCount --master spark://<master-ip-address>:7077 build/libs/WordCountExample-all.jar /Tom_Sawyer.txt
