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
    val nMonthsData = analyzeData.retrieveNMonthsData(noOfMonths)

    //nMonthsData.show(false)
    //nMonthsData.printSchema()

    if (analyticsOpt.equalsIgnoreCase("analytics1")){
      val (bestPerformingStoreByState, leastPerformingStoreByState) = analyzeData.storePerformanceByState(nMonthsData)
      bestPerformingStoreByState.show(false)
      leastPerformingStoreByState.show(false)
    }
    else if (analyticsOpt.equalsIgnoreCase("analytics2")){

    }
    else if (analyticsOpt.equalsIgnoreCase("analytics3")){

    }
  }
}
