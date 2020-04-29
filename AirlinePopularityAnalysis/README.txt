Author: Jack Fitzgerald
Date: 4/28/2020


How to build:

  - Navigate to the directory with build.gradle and execute 'gradle shadowJar'


How to execute:

  1. Set up a spark cluster with a few worker nodes

    - If you have a local Hadoop cluster setup, then stage the T-100 Market data on the Hadoop cluster first. 

  3. Example of executing a T-100 Market job: '$SPARK_HOME/bin/spark-submit --class DomesticPassengersFreightAndMailPerAirline --master spark://[INSERT MASTER IP ADDRESS HERE]:7077 build/libs/AirlinePopularityAnalysis-all.jar /t100_market_1990.csv /t100_market_1991.csv /t100_market_1992.csv /t100_market_1993.csv /t100_market_1994.csv'

    - This command will get the total number of passengers freight and mail per airline from 1990 to 1994

    - Any easy way to generate the filepath arguments is to use the following bash command: for((i=1990;i<2020;i++)); do echo -n "/t100_market_$i.csv"; done

    - NOTE: If the files submitted to the jobs are not from the T-100 Market data with ALL fields selected then unexpected behavior will ensue, the csv schema for all submitted files much match what is specified in t100_market_schema.txt

  4. Example of executing a Schedule P1.1/P1.2 job: '$SPARK_HOME/bin/spark-submit --class TotalNetIncomePerQuarter --master spark://[INSERT MASTER IP ADDRESS HERE]:7077 build/libs/AirlinePopularityAnalysis-all.jar /schedule_p12.csv'

    - This command will find the total net income per quarter from all airlines combined.

    - The file that is passed in as an argument must be Schedule P1.1 or Schedule P1.2 data with ONLY the NetIncome, UniqueCarrierName, Year, and Quarter fields selected.


File Descriptions:

  - T-100 Market:

    - DomesticPassengersFreightAndMailPerAirline.java: Spark job for finding the domestic total number of passengers, freight, and mail per airline per month. Can take 1 to many T-100 Market data files with ALL fields selected as arguments.

      - NOTE: This class can take a -u flag followed by a string of an airline carrier name to only look at data for that airline specifically. 

    - DomesticTotalPassengersFreightAndMailPerAirline.java: Spark job for finding the domestic total number of passengers, freight, and mail per airline across all files submitted as arguments. Files supplied as arguments must be from the T-100 Market data with ALL fields selected.

    - InternationalPassengersFreightAndMailPerAirline.java: Spark job for finding the international total number of passengers, freight, and mail per airline per airline per month. Can take 1 to many T-100 Market data files with ALL fields selected as arguments.

      - NOTE: This class can take a -u flag followed by a string of an airline carrier name to only look at data for that airline specifically.

    - InternationalTotalPassengersFreightAndMailPerAirline.java: Spark job for finding the international total number of passengers, freight, and mail per airline across all files submitted as arguments. Files supplied as arguments must be from the T-100 Market data with ALL fields selected.

  - Schedule P1.1 and Schedule P1.2:

    - TotalNetIncomePerQuarter.java: Spark job for finding the total net income per quarter from all airlines combined.

    - TotalNetIncomePerAirline.java: Spark job for finding the total net income for each airline.

    - TotalNetIncomeOfSpecificAirlinePerQuarter.java: Spark job for find the total net income of a specific airline for each quarter.

      - Example to find net income of Southwest Airlines: TotalNetIncomeOfSpecificAirlinePerQuarter "Southwest Airlines Inc." /schedule_p12.csv

