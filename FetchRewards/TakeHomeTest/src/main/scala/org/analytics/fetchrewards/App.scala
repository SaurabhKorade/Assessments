package org.analytics.fetchrewards

/**
 * Hello world!
 *
 */
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

    val originalUserReceiptsDf = sqlContext.read.
      format("csv").
      option("header", "true").
      option("inferSchema", "true").
      load("C:\\Users\\saura\\Documents\\MS\\BigData\\FetchRewardsData\\rewards_receipts_lat_v3.csv")

    originalUserReceiptsDf.show(false)

    if (analyticsOpt.equalsIgnoreCase("analytics1")){

    }
    else if (analyticsOpt.equalsIgnoreCase("analytics2")){

    }
    else if (analyticsOpt.equalsIgnoreCase("analytics3")){

    }
  }
}
