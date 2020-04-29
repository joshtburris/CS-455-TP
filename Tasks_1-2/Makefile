build:
	sbt package

task1:
	-rm -R ./T1-out
	-rm -R ~/spark-2.4.5-bin-hadoop2.7/work/*/
	-${HADOOP_HOME}/bin/hadoop fs -rm -R /tp/T1-out
	${SPARK_HOME}/bin/spark-submit --master spark://boise:30238 --deploy-mode cluster --class Task1 --supervise target/scala-2.11/tasks_2.11-1.0.jar

task2:
	-rm -R ./T2-out
	-rm -R ~/spark-2.4.5-bin-hadoop2.7/work/*
	-${HADOOP_HOME}/bin/hadoop fs -rm -R /tp/T2-out
	${SPARK_HOME}/bin/spark-submit --master spark://boise:30238 --deploy-mode cluster --class Task2 --supervise target/scala-2.11/tasks_2.11-1.0.jar

task3:
	-rm -R ./T3-out
	#-rm -R ~/spark-2.4.5-bin-hadoop2.7/work/*
	-${HADOOP_HOME}/bin/hadoop fs -rm -R /tp/T3-out
	${SPARK_HOME}/bin/spark-submit --master spark://boise:30238 --deploy-mode cluster --class Task3 --supervise target/scala-2.11/tasks_2.11-1.0.jar

getTask1:
	${HADOOP_HOME}/bin/hadoop fs -get /tp/T1-out ./T1-out

getTask2:
	${HADOOP_HOME}/bin/hadoop fs -get /tp/T2-out ./T2-out

getTask3:
	${HADOOP_HOME}/bin/hadoop fs -get /tp/T3-out ./T3-out

printTask1:
	for i in ./T1-out/* ; do cat $$i ; done

printTask2:
	for i in ./T2-out/* ; do cat $$i ; done

printTask3:
	for i in ./T3-out/* ; do cat $$i ; done
