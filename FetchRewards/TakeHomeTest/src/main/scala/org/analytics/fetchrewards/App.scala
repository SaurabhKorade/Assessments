package org.analytics.fetchrewards

object App {
  def main(args: Array[String]): Unit = {

    def isAllDigits(x: String) = x forall Character.isDigit

    //Check for correct arguments
    if(args.length != 6 || !(args(0).equalsIgnoreCase("analytics1") ||
      args(0).equalsIgnoreCase("analytics2") ||
      args(0).equalsIgnoreCase("analytics3")) ||
    (!isAllDigits(args(1))) || (args(3) == "" || args(4) == "" || args(5) == "")){
      println("-------------------------------Invalid args-------------------------------")
      println("Given args:", args.toList)
      println("Expected args: \n 1. Type of analytics (Options: analytics1, analytics2, analytics3)" +
      "\n 2. Number of months of data to be considered for analytics" +
      "\n 3. Expected output format (Options: csv, json, parquet)" +
      "\n 4. Output path to store result" +
      "\n 5. Input path to user_receipts data (rewards_receipts_lat_v3.csv)" +
      "\n 6. Input path to receipts_description data (rewards_receipt_item_lat_v2.csv)")
      sys.error("INVALID ARGS")
    }

    val analyticsOpt = args(0)
    val outputFormat = args(2)
    val outputPath = args(3)
    val noOfMonths = args(1).toInt
    val userDataInputPath = args(4)
    val receiptDescriptionInputPath = args(5)

    val appName = "AnalyticsService"
    val sparkConfig = new SparkConfManager

    //Objects to access spark configuration
    val sparkSession = sparkConfig.getSparkSession(appName)
    val sc = sparkConfig.getSparkContext(sparkSession)
    val sqlContext = sparkConfig.getSqlContext(sparkSession)

    val analyzeData = new AnalyzeData(sqlContext, sparkSession)
    val nMonthsDataDf = analyzeData.retrieveNMonthsData(noOfMonths, userDataInputPath, receiptDescriptionInputPath)
    val writeToDisk = new DiskWriter

    if (analyticsOpt.equalsIgnoreCase("analytics1")){
      val (bestPerformingStoreByState, leastPerformingStoreByState) = analyzeData.storePerformanceByState(nMonthsDataDf)
      println("---------------------------------------Analytics1---------------------------------------")
      bestPerformingStoreByState.show(false)
      leastPerformingStoreByState.show(false)
      writeToDisk.writeToDisk(bestPerformingStoreByState, outputFormat, outputPath+"1")
      writeToDisk.writeToDisk(leastPerformingStoreByState, outputFormat, outputPath+"2")
    }
    else if (analyticsOpt.equalsIgnoreCase("analytics2")){
      println("---------------------------------------Analytics2---------------------------------------")
      val top5SubCategoriesByStore = analyzeData.bestSelling5CategoriesByStore(nMonthsDataDf)
      top5SubCategoriesByStore.show(false)
      writeToDisk.writeToDisk(top5SubCategoriesByStore, outputFormat, outputPath)
    }
    else if (analyticsOpt.equalsIgnoreCase("analytics3")){
      println("---------------------------------------Analytics3---------------------------------------")
      val topSellingProductByState = analyzeData.countOfUsersPerProduct(nMonthsDataDf)
      topSellingProductByState.show(false)
      writeToDisk.writeToDisk(topSellingProductByState, outputFormat, outputPath)
    }
  }
}
