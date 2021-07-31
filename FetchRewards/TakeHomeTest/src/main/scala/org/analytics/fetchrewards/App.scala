package org.analytics.fetchrewards

object App {
  def main(args: Array[String]): Unit = {
    val analyticsOpt = args(0)
    val noOfMonths = args(1).toInt
    val outputFormat = args(2)

    val appName = "AnalyticsService"
    val sparkConfig = new SparkConfManager

    val sparkSession = sparkConfig.getSparkSession(appName)
    val sc = sparkConfig.getSparkContext(sparkSession)
    val sqlContext = sparkConfig.getSqlContext(sparkSession)

    val analyzeData = new AnalyzeData(sqlContext)
    val nMonthsData = analyzeData.retrieveNMonthsData(noOfMonths)

    nMonthsData.show(false)
    nMonthsData.printSchema()

    if (analyticsOpt.equalsIgnoreCase("analytics1")){

    }
    else if (analyticsOpt.equalsIgnoreCase("analytics2")){

    }
    else if (analyticsOpt.equalsIgnoreCase("analytics3")){

    }
  }
}
