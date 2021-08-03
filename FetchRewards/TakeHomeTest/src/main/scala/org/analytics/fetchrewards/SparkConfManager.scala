package org.analytics.fetchrewards

import org.apache.spark.sql.SparkSession

class SparkConfManager {
  //create spark session
  def getSparkSession(appName: String): SparkSession = {
    val sparkSession = SparkSession.builder()
//      .master("local")
      .appName(appName)
      .getOrCreate()
    sparkSession
  }

  //create spark context
  def getSparkContext(sparkSession: SparkSession) = {
    val sc = sparkSession.sparkContext
    sc
  }

  //create sqlContext
  def getSqlContext(sparkSession: SparkSession) = {
    val sqlContext = sparkSession.sqlContext
    sqlContext
  }

}
