package org.analytics.fetchrewards

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{max, months_between, round, udf}
import org.apache.spark.sql.{DataFrame, SQLContext}

class AnalyzeData (sqlContext: SQLContext) {
  import sqlContext.implicits._
  def retrieveNMonthsData(noOfMonths: Int): DataFrame ={
    val originalUserReceiptsDf = sqlContext.read.
      format("csv").
      option("header", "true").
      option("inferSchema", "true").
      load("C:\\Users\\saura\\Documents\\MS\\BigData\\FetchRewardsData\\rewards_receipts_lat_v3.csv")

    val originalReceiptDescriptionDf = sqlContext.read.
      format("csv").
      option("header", "true").
      option("inferSchema", "true").
      load("C:\\Users\\saura\\Documents\\MS\\BigData\\FetchRewardsData\\rewards_receipts_item_lat_v2.csv")

    val necessaryUserDataDF = originalUserReceiptsDf.select("RECEIPT_ID", "USER_ID", "STORE_NAME", "STORE_CITY", "STORE_STATE", "RECEIPT_PURCHASE_DATE", "RECEIPT_TOTAL", "RECEIPT_ITEM_COUNT")
    val necessaryReceiptDescDataDf = originalReceiptDescriptionDf.select("REWARDS_RECEIPT_ID", "RECEIPT_DESCRIPTION","QUANTITY", "ITEM_PRICE", "DISCOUNTED_PRICE", "BRAND", "CATEGORY")

    val maxDateWindow = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    val completeInfoDf = necessaryUserDataDF.
      join(necessaryReceiptDescDataDf, necessaryUserDataDF("RECEIPT_ID") === necessaryReceiptDescDataDf("REWARDS_RECEIPT_ID")).
      na.fill("Other", Seq("CATEGORY")).
      na.fill("Other", Seq("STORE_STATE")).
      withColumn("MAX_DATE", max("RECEIPT_PURCHASE_DATE").over(maxDateWindow)).
      withColumn("DATE_DIFF", round(months_between($"MAX_DATE", $"RECEIPT_PURCHASE_DATE"))).
      drop("REWARDS_RECEIPT_ID", "MAX_DATE")

    val getSubCategory =  (strCategory:String) => {
      val arr = strCategory.split("\\|")
      if(arr.size >= 2)
        arr(1)
      else
        arr(0)
    }

    val getSubCategoryUDF = udf(getSubCategory)

    val nMonthsDataDf = completeInfoDf.
      filter(completeInfoDf("DATE_DIFF") >= noOfMonths).
      withColumn("SUB_CATEGORY", getSubCategoryUDF('CATEGORY)).
      drop("DATE_DIFF", "CATEGORY")

    nMonthsDataDf

  }

}
