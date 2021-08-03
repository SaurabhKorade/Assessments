#Overview
This spark application runs the following analytics queries and is coded in scala using a maven project. Input files are provided in the resources folder.

##Assignment URL: 
https://fetch-hiring.s3.amazonaws.com/spark-engineer/spark-challenge.html

##Identified analytics queries:
Analytics 1: Find best and least performing store (as per revenue) by state  
Analytics 2: Find % contribution of top 5 selling sub categories for each store  
Analytics 3: Find best-selling items (as per customer count) by state  

##Expected arguments:
 1. Type of analytics (Options: analytics1, analytics2, analytics3)
 2. Number of months of data to be considered for analytics
 3. Expected output format (Options: csv, json, parquet)
 4. Output path to store result
 5. Input path to user_receipts data (rewards_receipts_lat_v3.csv)
 6. Input path to receipts_description data (rewards_receipt_item_lat_v2.csv)

##How to run:

The project can be launched on the cluster using spark-submit.

If you do not have a cluster but want to quickly test or run in local mode you can use the following:
````
.\spark-submit --class org.analytics.fetchrewards.App --master local .\TakeHomeTest-1.0-SNAPSHOT.jar analytics1 20 json \outputPath1 rewards_receipts_lat_v3 rewards_receipt_item_lat_v2
````
Examples of different run commands:
````
.\spark-submit --class org.analytics.fetchrewards.App --master local .\TakeHomeTest-1.0-SNAPSHOT.jar analytics1 10 json \outputPath1  rewards_receipts_lat_v3 rewards_receipt_item_lat_v2
.\spark-submit --class org.analytics.fetchrewards.App --master local .\TakeHomeTest-1.0-SNAPSHOT.jar analytics2 7 csv \outputPath1 rewards_receipts_lat_v3 rewards_receipt_item_lat_v2
.\spark-submit --class org.analytics.fetchrewards.App --master local .\TakeHomeTest-1.0-SNAPSHOT.jar analytics3 31 parquet \outputPath1 rewards_receipts_lat_v3 rewards_receipt_item_lat_v2
````
**Note:**
1. You do not need to build the project, and can you use the existing jar from the git repo target directory.
2. Basic validation is done, but I have assumed that the arguments are submitted in the same order as mentioned above.
3. Change the path to output for every run to avoid getting "file already exists" error as I do not clean previous output.
4. Specify complete path to the TakeHomeTest-1.0-SNAPSHOT.jar, rewards_receipts_lat_v3, rewards_receipt_item_lat_v2 if not running the spark-submit command from where the files reside.
5. If you wish to build the project and use a new jar you can do so using maven.
6. Analytics query 2 generates 2 separate outputs with names /output_path1 and /output_path2 for best-performing and least-performing stores by state.
 