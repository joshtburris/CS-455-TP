Author: Jack Fitzgerald
Date: 4/28/2020


How to build:

  - Navigate to the directory with build.gradle and execute 'gradle shadowJar'


How to execute:

  1. Set up a spark cluster with a few worker nodes

    - If you have a local Hadoop cluster setup, then stage the T-100 Market data on the Hadoop cluster first. 

  3. Example: execute '$SPARK_HOME/bin/spark-submit --class DomesticPassengersFreightAndMailPerAirline --master spark://[INSERT MASTER IP ADDRESS HERE]:7077 build/libs/AirlinePopularityAnalysis-all.jar /t100_market_1990.csv /t100_market_1991.csv /t100_market_1992.csv /t100_market_1993.csv /t100_market_1994.csv'

    - This command will get the total number of passengers freight and mail per airline from 1990 to 1994

    - Any easy way to generate the filepath arguments is to use the following bash command: for((i=1990;i<2020;i++)); do echo -n "/t100_market_$i.csv"; done

    - NOTE: If the files submitted to the jobs are not from the T-100 Market data with ALL fields selected then unexpected behavior will ensue, the csv schema for all submitted files much match what is specified in t100_market_schema.txt


File Descriptions:

  - DomesticPassengersFreightAndMailPerAirline.java: Spark job for finding the domestic total number of passengers, freight, and mail per airline per month. Can take 1 to many T-100 Market data files with ALL fields selected as arguments.

  - DomesticTotalPassengersFreightAndMailPerAirline.java: Spark job for finding the domestic total number of passengers, freight, and mail per airline across all files submitted as arguments. Files supplied as arguments must be from the T-100 Market data with ALL fields selected.

  - InternationalPassengersFreightAndMailPerAirline.java: Spark job for finding the international total number of passengers, freight, and mail per airline per airline per month. Can take 1 to many T-100 Market data files with ALL fields selected as arguments.

  - InternationalTotalPassengersFreightAndMailPerAirline.java: Spark job for finding the international total number of passengers, freight, and mail per airline across all files submitted as arguments. Files supplied as arguments must be from the T-100 Market data with ALL fields selected.






