package org.analytics.fetchrewards

object App {
  def main(args: Array[String]): Unit = {

    //Add validation for the following fields
    val analyticsOpt = args(0)
    val noOfMonths = args(1).toInt
    val outputFormat = args(2)

    val appName = "AnalyticsService"
    val sparkConfig = new SparkConfManager

    //Objects to access spark configuration
    val sparkSession = sparkConfig.getSparkSession(appName)
    val sc = sparkConfig.getSparkContext(sparkSession)
    val sqlContext = sparkConfig.getSqlContext(sparkSession)

    val analyzeData = new AnalyzeData(sqlContext)
    val nMonthsDataDf = analyzeData.retrieveNMonthsData(noOfMonths)
    val writeToDisk = new DiskWriter

    //nMonthsData.show(false)
    //nMonthsData.printSchema()

    if (analyticsOpt.equalsIgnoreCase("analytics1")){
      val (bestPerformingStoreByState, leastPerformingStoreByState) = analyzeData.storePerformanceByState(nMonthsDataDf)
      writeToDisk.writeToDisk(bestPerformingStoreByState, outputFormat)
      writeToDisk.writeToDisk(leastPerformingStoreByState, outputFormat)
    }
    else if (analyticsOpt.equalsIgnoreCase("analytics2")){
      val top5SubCategoriesByStore = analyzeData.bestSelling5CategoriesByStore(nMonthsDataDf)
//      writeToDisk.writeToDisk(top5SubCategoriesByStore, outputFormat)
      top5SubCategoriesByStore.show(false)
    }
    else if (analyticsOpt.equalsIgnoreCase("analytics3")){

    }
  }
}
