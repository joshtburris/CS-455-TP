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
